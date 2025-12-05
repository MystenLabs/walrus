// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React from "react";
import type { Props } from "@theme/CodeBlock";
import OriginalCodeBlock from "@theme-original/CodeBlock";

export default function CodeBlock(props: Props) {
    // Delegate to the theme's original component so we keep its Context provider
    // and any built-in behavior, while still allowing future customizations here.
    return <OriginalCodeBlock {...props} />;
}
