// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use opentelemetry_sdk::trace::TracerProvider as SdkTracerProvider;
use tracing::subscriber::DefaultGuard;

/// Guard which performs any shutdown actions for a subscriber on drop.
#[derive(Default, Debug)]
pub struct SubscriberGuard {
    tracer_provider: Option<SdkTracerProvider>,
    default_guard: Option<DefaultGuard>,
}

impl SubscriberGuard {
    pub(crate) fn with_default_guard(mut self, guard: DefaultGuard) -> Self {
        self.default_guard = Some(guard);
        self
    }

    pub(crate) fn otlp(provider: SdkTracerProvider) -> Self {
        Self {
            tracer_provider: Some(provider),
            default_guard: None,
        }
    }
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take() {
            for result in provider.force_flush() {
                if let Err(err) = result {
                    eprintln!("{err:?}");
                }
            }

            if let Err(err) = provider.shutdown() {
                eprintln!("{err:?}");
            }
        }
    }
}
