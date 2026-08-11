// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React from "react";

/**
 * Inline entry point for the Ask Walrus AI assistant, for placement on
 * high-traffic documentation pages. Opens the same Kapa sidebar as the
 * navbar trigger.
 */
export default function AskAiCta({
    prompt = "Stuck or curious? Ask Walrus AI — it answers from this documentation.",
}: {
    prompt?: string;
}) {
    const handleClick = () => {
        if (typeof window !== "undefined" && (window as any).Kapa) {
            (window as any).Kapa.open();
        }
    };

    return (
        <div className="askai-cta">
            <span className="askai-cta-text">{prompt}</span>
            <button type="button" className="askai-cta-button" onClick={handleClick}>
                <img src="/img/logo.svg" alt="" width="16" height="16" />
                Ask Walrus AI
            </button>
        </div>
    );
}
