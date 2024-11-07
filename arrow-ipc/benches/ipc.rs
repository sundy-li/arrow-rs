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

use arrow_array::{ArrayRef, LargeStringArray, RecordBatch, StringViewArray};
use arrow_ipc::{reader::FileReaderBuilder, writer::FileWriter};
use arrow_schema::{DataType, Field, Schema};
use criterion::*;
use rand::{thread_rng, Rng};
use std::sync::Arc;

#[allow(deprecated)]
fn do_bench(c: &mut Criterion, name: &str, array: ArrayRef, schema: &Schema) {
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![array]).unwrap();
    c.bench_function(name, |b| {
        b.iter(|| {
            //write
            let mut buffer = Vec::new();
            let mut fw = FileWriter::try_new(&mut buffer, schema).unwrap();
            fw.write(&batch).unwrap();
            fw.finish().unwrap();

            // read
            let cursor = std::io::Cursor::new(buffer.as_slice());
            let mut reader = FileReaderBuilder::new().build(cursor).unwrap();
            let result = reader.next().unwrap().unwrap();
            assert_eq!(result, batch);
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = thread_rng();
    for length in [12, 20, 500] {
        let iter = (0..102400).map(|_| {
            let random_string: String = (0..length)
                .map(|_| {
                    // Generate a random character (ASCII printable characters)
                    rng.gen_range(32..=126) as u8 as char
                })
                .collect();
            random_string
        });

        let schema = Schema::new(vec![Field::new("large_utf8", DataType::LargeUtf8, false)]);
        let array = LargeStringArray::from_iter_values(iter);

        let name = format!("ipc_serde_large_utf8_{}", length);
        do_bench(c, &name, Arc::new(array) as _, &schema);

        let iter = (0..102400).map(|_| {
            let random_string: String = (0..length)
                .map(|_| {
                    // Generate a random character (ASCII printable characters)
                    rng.gen_range(32..=126) as u8 as char
                })
                .collect();
            random_string
        });
        let schema = Schema::new(vec![Field::new("utf8_view", DataType::Utf8View, false)]);
        let array = StringViewArray::from_iter_values(iter);
        let name = format!("ipc_serde_utf8_view_{}", length);
        do_bench(c, &name, Arc::new(array) as _, &schema);
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
