// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_flight::sql::{
    ActionCreatePreparedStatementResult, Any, ProstMessageExt, SqlInfo,
};
use arrow_flight::{
    Action, FlightData, FlightEndpoint, HandshakeRequest, HandshakeResponse, IpcMessage,
    Location, SchemaAsIpc, Ticket,
};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use futures::{stream, Stream};
use prost::Message;
use std::pin::Pin;
use std::sync::Arc;
use tonic::transport::Server;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    flight_service_server::FlightService,
    flight_service_server::FlightServiceServer,
    sql::{
        server::FlightSqlService, ActionClosePreparedStatementRequest,
        ActionCreatePreparedStatementRequest, CommandGetCatalogs,
        CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys,
        CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo,
        CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
        CommandPreparedStatementQuery, CommandPreparedStatementUpdate,
        CommandStatementQuery, CommandStatementUpdate, TicketStatementQuery,
    },
    FlightDescriptor, FlightInfo,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{ArrowError, DataType, Field, Schema};

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

const FAKE_TOKEN: &str = "uuid_token";
const FAKE_HANDLE: &str = "uuid_handle";
const FAKE_UPDATE_RESULT: i64 = 1;

#[derive(Clone)]
pub struct FlightSqlServiceImpl {}

impl FlightSqlServiceImpl {
    fn check_token<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let metadata = req.metadata();
        let auth = metadata.get("authorization").ok_or_else(|| {
            Status::internal(format!("No authorization header! metadata = {metadata:?}"))
        })?;
        let str = auth
            .to_str()
            .map_err(|e| Status::internal(format!("Error parsing header: {e}")))?;
        let authorization = str.to_string();
        let bearer = "Bearer ";
        if !authorization.starts_with(bearer) {
            Err(Status::internal("Invalid auth header!"))?;
        }
        let token = authorization[bearer.len()..].to_string();
        if token == FAKE_TOKEN {
            Ok(())
        } else {
            Err(Status::unauthenticated("invalid token "))
        }
    }

    fn fake_result() -> Result<RecordBatch, ArrowError> {
        let schema = Schema::new(vec![Field::new("salutation", DataType::Utf8, false)]);
        let mut builder = StringBuilder::new();
        builder.append_value("Hello, FlightSQL!");
        let cols = vec![Arc::new(builder.finish()) as ArrayRef];
        RecordBatch::try_new(Arc::new(schema), cols)
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let basic = "Basic ";
        let authorization = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::invalid_argument("authorization field not present"))?
            .to_str()
            .map_err(|e| status!("authorization not parsable", e))?;
        if !authorization.starts_with(basic) {
            Err(Status::invalid_argument(format!(
                "Auth type not implemented: {authorization}"
            )))?;
        }
        let base64 = &authorization[basic.len()..];
        let bytes = BASE64_STANDARD
            .decode(base64)
            .map_err(|e| status!("authorization not decodable", e))?;
        let str = String::from_utf8(bytes)
            .map_err(|e| status!("authorization not parsable", e))?;
        let parts: Vec<_> = str.split(':').collect();
        let (user, pass) = match parts.as_slice() {
            [user, pass] => (user, pass),
            _ => Err(Status::invalid_argument(
                "Invalid authorization header".to_string(),
            ))?,
        };
        if user != &"admin" || pass != &"password" {
            Err(Status::unauthenticated("Invalid credentials!"))?
        }

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: FAKE_TOKEN.into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        return Ok(Response::new(Box::pin(output)));
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        _message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.check_token(&request)?;
        let batch =
            Self::fake_result().map_err(|e| status!("Could not fake a result", e))?;
        let schema = (*batch.schema()).clone();
        let batches = vec![batch];
        let flight_data = batches_to_flight_data(schema, batches)
            .map_err(|e| status!("Could not convert batches", e))?
            .into_iter()
            .map(Ok);

        let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
            Box::pin(stream::iter(flight_data));
        let resp = Response::new(stream);
        Ok(resp)
    }

    async fn get_flight_info_statement(
        &self,
        _query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_statement not implemented",
        ))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check_token(&request)?;
        let handle = std::str::from_utf8(&cmd.prepared_statement_handle)
            .map_err(|e| status!("Unable to parse handle", e))?;
        let batch =
            Self::fake_result().map_err(|e| status!("Could not fake a result", e))?;
        let schema = (*batch.schema()).clone();
        let num_rows = batch.num_rows();
        let num_bytes = batch.get_array_memory_size();
        let loc = Location {
            uri: "grpc+tcp://127.0.0.1".to_string(),
        };
        let fetch = FetchResults {
            handle: handle.to_string(),
        };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![loc],
        };
        let endpoints = vec![endpoint];

        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Default::default(),
            path: vec![],
        };
        let info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(flight_desc),
            endpoint: endpoints,
            total_records: num_rows as i64,
            total_bytes: num_bytes as i64,
        };
        let resp = Response::new(info);
        Ok(resp)
    }

    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_catalogs not implemented",
        ))
    }

    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_schemas not implemented",
        ))
    }

    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_tables not implemented",
        ))
    }

    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_table_types not implemented",
        ))
    }

    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_sql_info not implemented",
        ))
    }

    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_primary_keys not implemented",
        ))
    }

    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_exported_keys not implemented",
        ))
    }

    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_xdbc_type_info not implemented",
        ))
    }

    // do_get
    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_statement not implemented"))
    }

    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_prepared_statement not implemented",
        ))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_catalogs not implemented"))
    }

    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_schemas not implemented"))
    }

    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_tables not implemented"))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_table_types not implemented"))
    }

    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_sql_info not implemented"))
    }

    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_primary_keys not implemented"))
    }

    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_exported_keys not implemented",
        ))
    }

    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_imported_keys not implemented",
        ))
    }

    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_cross_reference not implemented",
        ))
    }

    async fn do_get_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_xdbc_type_info not implemented",
        ))
    }

    // do_put
    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        Ok(FAKE_UPDATE_RESULT)
    }

    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "do_put_prepared_statement_query not implemented",
        ))
    }

    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_prepared_statement_update not implemented",
        ))
    }

    async fn do_action_create_prepared_statement(
        &self,
        _query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        self.check_token(&request)?;
        let schema = Self::fake_result()
            .map_err(|e| status!("Error getting result schema", e))?
            .schema();
        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;
        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: FAKE_HANDLE.into(),
            dataset_schema: schema_bytes,
            parameter_schema: Default::default(), // TODO: parameters
        };
        Ok(res)
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) {
        unimplemented!("Implement do_action_close_prepared_statement")
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// This example shows how to run a FlightSql server
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;

    let svc = FlightServiceServer::new(FlightSqlServiceImpl {});

    println!("Listening on {:?}", addr);

    if std::env::var("USE_TLS").ok().is_some() {
        let cert = std::fs::read_to_string("arrow-flight/examples/data/server.pem")?;
        let key = std::fs::read_to_string("arrow-flight/examples/data/server.key")?;
        let client_ca =
            std::fs::read_to_string("arrow-flight/examples/data/client_ca.pem")?;

        let tls_config = ServerTlsConfig::new()
            .identity(Identity::from_pem(&cert, &key))
            .client_ca_root(Certificate::from_pem(&client_ca));

        Server::builder()
            .tls_config(tls_config)?
            .add_service(svc)
            .serve(addr)
            .await?;
    } else {
        Server::builder().add_service(svc).serve(addr).await?;
    }

    Ok(())
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResults {
    #[prost(string, tag = "1")]
    pub handle: ::prost::alloc::string::String,
}

impl ProstMessageExt for FetchResults {
    fn type_url() -> &'static str {
        "type.googleapis.com/arrow.flight.protocol.sql.FetchResults"
    }

    fn as_any(&self) -> Any {
        Any {
            type_url: FetchResults::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use std::fs;
    use std::future::Future;
    use std::time::Duration;
    use tempfile::NamedTempFile;
    use tokio::net::{UnixListener, UnixStream};
    use tokio::time::sleep;
    use tokio_stream::wrappers::UnixListenerStream;
    use tonic::transport::{Channel, ClientTlsConfig};

    use arrow_cast::pretty::pretty_format_batches;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use arrow_flight::utils::flight_data_to_batches;
    use tonic::transport::{Certificate, Endpoint};
    use tower::service_fn;

    async fn client_with_uds(path: String) -> FlightSqlServiceClient<Channel> {
        let connector = service_fn(move |_| UnixStream::connect(path.clone()));
        let channel = Endpoint::try_from("http://example.com")
            .unwrap()
            .connect_with_connector(connector)
            .await
            .unwrap();
        FlightSqlServiceClient::new(channel)
    }

    async fn create_https_server() -> Result<(), tonic::transport::Error> {
        let cert = std::fs::read_to_string("examples/data/server.pem").unwrap();
        let key = std::fs::read_to_string("examples/data/server.key").unwrap();
        let client_ca = std::fs::read_to_string("examples/data/client_ca.pem").unwrap();

        let tls_config = ServerTlsConfig::new()
            .identity(Identity::from_pem(&cert, &key))
            .client_ca_root(Certificate::from_pem(&client_ca));

        let addr = "0.0.0.0:50051".parse().unwrap();

        let svc = FlightServiceServer::new(FlightSqlServiceImpl {});

        Server::builder()
            .tls_config(tls_config)
            .unwrap()
            .add_service(svc)
            .serve(addr)
            .await
    }

    fn endpoint(addr: String) -> Result<Endpoint, ArrowError> {
        let endpoint = Endpoint::new(addr)
            .map_err(|_| ArrowError::IoError("Cannot create endpoint".to_string()))?
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(20))
            .tcp_nodelay(true) // Disable Nagle's Algorithm since we don't want packets to wait
            .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
            .http2_keep_alive_interval(Duration::from_secs(300))
            .keep_alive_timeout(Duration::from_secs(20))
            .keep_alive_while_idle(true);

        Ok(endpoint)
    }

    #[tokio::test]
    async fn test_select_https() {
        tokio::spawn(async {
            create_https_server().await.unwrap();
        });

        sleep(Duration::from_millis(2000)).await;

        let request_future = async {
            let cert = std::fs::read_to_string("examples/data/client1.pem").unwrap();
            let key = std::fs::read_to_string("examples/data/client1.key").unwrap();
            let server_ca = std::fs::read_to_string("examples/data/ca.pem").unwrap();

            let tls_config = ClientTlsConfig::new()
                .domain_name("localhost")
                .ca_certificate(Certificate::from_pem(&server_ca))
                .identity(Identity::from_pem(cert, key));
            let endpoint = endpoint(String::from("https://127.0.0.1:50051"))
                .unwrap()
                .tls_config(tls_config)
                .unwrap();
            let channel = endpoint.connect().await.unwrap();
            let mut client = FlightSqlServiceClient::new(channel);
            let token = client.handshake("admin", "password").await.unwrap();
            client.set_token(String::from_utf8(token.to_vec()).unwrap());
            println!("Auth succeeded with token: {:?}", token);
            let mut stmt = client.prepare("select 1;".to_string()).await.unwrap();
            let flight_info = stmt.execute().await.unwrap();
            let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap().clone();
            let flight_data = client.do_get(ticket).await.unwrap();
            let flight_data: Vec<FlightData> = flight_data.try_collect().await.unwrap();
            let batches = flight_data_to_batches(&flight_data).unwrap();
            let res = pretty_format_batches(batches.as_slice()).unwrap();
            let expected = r#"
+-------------------+
| salutation        |
+-------------------+
| Hello, FlightSQL! |
+-------------------+"#
                .trim()
                .to_string();
            assert_eq!(res.to_string(), expected);
        };

        tokio::select! {
            _ = request_future => println!("Client finished!"),
        }
    }

    async fn auth_client(client: &mut FlightSqlServiceClient<Channel>) {
        let token = client.handshake("admin", "password").await.unwrap();
        client.set_token(String::from_utf8(token.to_vec()).unwrap());
    }

    async fn test_client<F, C>(f: F)
    where
        F: FnOnce(FlightSqlServiceClient<Channel>) -> C,
        C: Future<Output = ()>,
    {
        let file = NamedTempFile::new().unwrap();
        let path = file.into_temp_path().to_str().unwrap().to_string();
        let _ = fs::remove_file(path.clone());

        let uds = UnixListener::bind(path.clone()).unwrap();
        let stream = UnixListenerStream::new(uds);

        // We would just listen on TCP, but it seems impossible to know when tonic is ready to serve
        let service = FlightSqlServiceImpl {};
        let serve_future = Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(stream);

        let request_future = async {
            let client = client_with_uds(path).await;
            f(client).await
        };

        tokio::select! {
            _ = serve_future => panic!("server returned first"),
            _ = request_future => println!("Client finished!"),
        }
    }

    #[tokio::test]
    async fn test_select_1() {
        test_client(|mut client| async move {
            auth_client(&mut client).await;
            let mut stmt = client.prepare("select 1;".to_string()).await.unwrap();
            let flight_info = stmt.execute().await.unwrap();
            let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap().clone();
            let flight_data = client.do_get(ticket).await.unwrap();
            let flight_data: Vec<FlightData> = flight_data.try_collect().await.unwrap();
            let batches = flight_data_to_batches(&flight_data).unwrap();
            let res = pretty_format_batches(batches.as_slice()).unwrap();
            let expected = r#"
+-------------------+
| salutation        |
+-------------------+
| Hello, FlightSQL! |
+-------------------+"#
                .trim()
                .to_string();
            assert_eq!(res.to_string(), expected);
        })
        .await
    }

    #[tokio::test]
    async fn test_execute_update() {
        test_client(|mut client| async move {
            auth_client(&mut client).await;
            let res = client
                .execute_update("creat table test(a int);".to_string())
                .await
                .unwrap();
            assert_eq!(res, FAKE_UPDATE_RESULT);
        })
        .await
    }

    #[tokio::test]
    async fn test_auth() {
        test_client(|mut client| async move {
            // no handshake
            assert!(client
                .prepare("select 1;".to_string())
                .await
                .unwrap_err()
                .to_string()
                .contains("No authorization header"));

            // Invalid credentials
            assert!(client
                .handshake("admin", "password2")
                .await
                .unwrap_err()
                .to_string()
                .contains("Invalid credentials"));

            // forget to set_token
            client.handshake("admin", "password").await.unwrap();
            assert!(client
                .prepare("select 1;".to_string())
                .await
                .unwrap_err()
                .to_string()
                .contains("No authorization header"));

            // Invalid Tokens
            client.handshake("admin", "password").await.unwrap();
            client.set_token("wrong token".to_string());
            assert!(client
                .prepare("select 1;".to_string())
                .await
                .unwrap_err()
                .to_string()
                .contains("invalid token"));
        })
        .await
    }
}
