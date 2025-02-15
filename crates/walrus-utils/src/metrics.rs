// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[macro_export]
macro_rules! with_label {
    ($metric:expr, $label:expr) => {
        $metric.with_label_values(&[$label.as_ref()])
    };
    ($metric:expr, $label1:expr, $label2:expr) => {
        $metric.with_label_values(&[$label1.as_ref(), $label2.as_ref()])
    };
    ($metric:expr, $label1:expr, $label2:expr, $label3:expr) => {
        $metric.with_label_values(&[$label1.as_ref(), $label2.as_ref(), $label3.as_ref()])
    };
}
