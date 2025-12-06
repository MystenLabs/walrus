// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React, { type ReactNode } from "react";
import Link from "@docusaurus/Link";
import type { Props } from "@theme/TOCItems/Tree";
import PushFeedback from "../../components/PushFeedback";

// Recursive component rendering the toc tree
function TOCItemTree({ toc, className, linkClassName, isChild }: Props): ReactNode {
    if (!toc.length) {
        return null;
    }
    return (
        <ul className={isChild ? undefined : className}>
            {toc.map((heading) => (
                <li key={heading.id}>
                    <Link
                        to={`#${heading.id}`}
                        className={linkClassName ?? undefined}
                        // Developer provided the HTML, so assume it's safe.
                        dangerouslySetInnerHTML={{ __html: heading.value }}
                    />
                    <TOCItemTree
                        isChild
                        toc={heading.children}
                        className={className}
                        linkClassName={linkClassName}
                    />
                </li>
            ))}
            {!isChild && (
                <li className="border border-solid border-transparent !mt-4 border-t-suins-gray-20 dark:border-t-suins-white-20">
                    <PushFeedback />
                </li>
            )}
        </ul>
    );
}

// Memo only the tree root is enough
export default React.memo(TOCItemTree);
