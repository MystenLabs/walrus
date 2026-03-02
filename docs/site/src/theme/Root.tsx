// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React from "react";
import GlossaryProvider from "@site/src/shared/components/Glossary/GlossaryProvider";
import "../css/fontawesome";

export default function Root({ children }: { children: React.ReactNode }) {
    return (
        <>
            <noscript>
                <iframe
                    src="https://www.googletagmanager.com/ns.html?id=GTM-M73JK866"
                    height="0"
                    width="0"
                    style={{ display: "none", visibility: "hidden" }}
                />
            </noscript>
            <GlossaryProvider>{children}</GlossaryProvider>
        </>
    );
}