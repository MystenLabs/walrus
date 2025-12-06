// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React from "react";
import GlossaryProvider from "@site/src/components/Glossary/GlossaryProvider";
import "../css/fontawesome";

export default function Root({ children }: { children: React.ReactNode }) {
    return <GlossaryProvider>{children}</GlossaryProvider>;
}
