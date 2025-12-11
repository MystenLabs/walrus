// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React, { useEffect } from "react";
import GlossaryProvider from "@site/src/components/Glossary/GlossaryProvider";
import "../css/fontawesome";

export default function Root({ children }: { children: React.ReactNode }) {
    useEffect(() => {
        const fixMobileMargins = () => {
            if (window.innerWidth <= 996) {
                // Remove all mr-* margin classes on mobile
                document.querySelectorAll('[class*="mr-"]').forEach(el => {
                    (el as HTMLElement).style.setProperty('margin-right', '0', 'important');
                });

                // Specifically target the main content container
                document.querySelectorAll('.flex-1, [class*="flex-1"]').forEach(el => {
                    (el as HTMLElement).style.setProperty('margin-right', '0', 'important');
                    (el as HTMLElement).style.setProperty('padding-left', '1rem', 'important');
                    (el as HTMLElement).style.setProperty('padding-right', '1rem', 'important');
                });
            }
        };

        const fixNavbar = () => {
            // Apply these fixes on all screen sizes for consistent alignment

            // Fix navbar search container alignment
            document.querySelectorAll('.navbarSearchContainer_XBZY').forEach(el => {
                (el as HTMLElement).style.setProperty('display', 'flex', 'important');
                (el as HTMLElement).style.setProperty('align-items', 'center', 'important');
                (el as HTMLElement).style.setProperty('gap', '0.5rem', 'important');
            });

            // Align Ask Cookbook container - remove negative margin
            document.querySelectorAll('#ask-cookbook-container').forEach(el => {
                (el as HTMLElement).style.setProperty('margin', '0', 'important');
                (el as HTMLElement).style.setProperty('display', 'flex', 'important');
                (el as HTMLElement).style.setProperty('align-items', 'center', 'important');
            });

            // Target the button inside shadow DOM via querySelector
            const cookbookContainer = document.querySelector('#ask-cookbook-container');
            if (cookbookContainer) {
                const askCookbook = cookbookContainer.querySelector('ask-cookbook');
                if (askCookbook && askCookbook.shadowRoot) {
                    const button = askCookbook.shadowRoot.querySelector('#ask-cookbook-button');
                    if (button instanceof HTMLElement) {
                        button.style.setProperty('height', '32px', 'important');
                        button.style.setProperty('min-height', '32px', 'important');
                        button.style.setProperty('max-height', '32px', 'important');
                        button.style.setProperty('padding', '0.25rem 0.5rem', 'important');
                        button.style.setProperty('line-height', '1.2', 'important');
                        button.style.setProperty('display', 'flex', 'important');
                        button.style.setProperty('align-items', 'center', 'important');
                        button.style.setProperty('margin', '0', 'important');
                    }
                }
            }

            // Make cookbook button same size as search button (fallback for non-shadow DOM)
            document.querySelectorAll('#ask-cookbook-button').forEach(el => {
                (el as HTMLElement).style.setProperty('height', '32px', 'important');
                (el as HTMLElement).style.setProperty('min-height', '32px', 'important');
                (el as HTMLElement).style.setProperty('max-height', '32px', 'important');
                (el as HTMLElement).style.setProperty('padding', '0.25rem 0.5rem', 'important');
                (el as HTMLElement).style.setProperty('line-height', '1.2', 'important');
                (el as HTMLElement).style.setProperty('display', 'flex', 'important');
                (el as HTMLElement).style.setProperty('align-items', 'center', 'important');
                (el as HTMLElement).style.setProperty('margin', '0', 'important');
            });

            // Ensure search button has consistent sizing
            document.querySelectorAll('.DocSearch-Button').forEach(el => {
                (el as HTMLElement).style.setProperty('height', '32px', 'important');
                (el as HTMLElement).style.setProperty('min-height', '32px', 'important');
                (el as HTMLElement).style.setProperty('max-height', '32px', 'important');
                (el as HTMLElement).style.setProperty('padding', '0.25rem 0.5rem', 'important');
                (el as HTMLElement).style.setProperty('line-height', '1.2', 'important');
            });

            if (window.innerWidth <= 996) {
                // Mobile-specific fixes

                // Fix navbar items wrapping
                document.querySelectorAll('.navbar__items').forEach(el => {
                    (el as HTMLElement).style.setProperty('flex-wrap', 'wrap', 'important');
                    (el as HTMLElement).style.setProperty('gap', '0.5rem', 'important');
                });

                // Additional mobile-specific navbar search container fixes
                document.querySelectorAll('.navbarSearchContainer_XBZY').forEach(el => {
                    (el as HTMLElement).style.setProperty('flex-wrap', 'nowrap', 'important');
                    (el as HTMLElement).style.setProperty('max-width', '100%', 'important');
                });

                // Scale down cookbook button font on mobile (shadow DOM)
                if (cookbookContainer) {
                    const askCookbook = cookbookContainer.querySelector('ask-cookbook');
                    if (askCookbook && askCookbook.shadowRoot) {
                        const button = askCookbook.shadowRoot.querySelector('#ask-cookbook-button');
                        if (button instanceof HTMLElement) {
                            button.style.setProperty('font-size', '0.875rem', 'important');
                        }
                    }
                }

                // Scale down cookbook button font on mobile (fallback)
                document.querySelectorAll('#ask-cookbook-button').forEach(el => {
                    (el as HTMLElement).style.setProperty('font-size', '0.875rem', 'important');
                    (el as HTMLElement).style.setProperty('flex-shrink', '1', 'important');
                });

                // Scale down search button on mobile
                document.querySelectorAll('.DocSearch-Button').forEach(el => {
                    (el as HTMLElement).style.setProperty('max-width', '120px', 'important');
                    (el as HTMLElement).style.setProperty('font-size', '0.875rem', 'important');
                });

                // Hide GitHub icon on mobile
                document.querySelectorAll('.header-github-link').forEach(el => {
                    (el as HTMLElement).style.setProperty('display', 'none', 'important');
                });

                // Ensure right items don't overflow
                document.querySelectorAll('.navbar__items--right').forEach(el => {
                    (el as HTMLElement).style.setProperty('flex-wrap', 'wrap', 'important');
                    (el as HTMLElement).style.setProperty('max-width', '100%', 'important');
                });

                // Hide Cookbook AI popup/modal on mobile to prevent layout issues
                document.querySelectorAll('.sc-1o3hmkg-0').forEach(el => {
                    (el as HTMLElement).style.setProperty('display', 'none', 'important');
                });

                // Hide any Monaco editor instances in navbar
                document.querySelectorAll('.monaco-editor').forEach(el => {
                    (el as HTMLElement).style.setProperty('display', 'none', 'important');
                });

                // Hide cookbook modal containers
                document.querySelectorAll('[aria-expanded="true"][aria-haspopup="listbox"]').forEach(el => {
                    if ((el as HTMLElement).classList.contains('sc-1o3hmkg-0')) {
                        (el as HTMLElement).style.setProperty('display', 'none', 'important');
                    }
                });
            } else {
                // Reset GitHub button on larger screens
                document.querySelectorAll('.header-github-link').forEach(el => {
                    (el as HTMLElement).style.removeProperty('display');
                });
            }
        };

        const applyFixes = () => {
            fixMobileMargins();
            fixNavbar();
        };

        // Run immediately
        applyFixes();

        // Run on resize
        window.addEventListener('resize', applyFixes);

        // Run on route change and DOM updates
        let timeoutId: NodeJS.Timeout;
        const observer = new MutationObserver(() => {
            clearTimeout(timeoutId);
            timeoutId = setTimeout(applyFixes, 100);
        });

        observer.observe(document.body, {
            childList: true,
            subtree: true
        });

        return () => {
            window.removeEventListener('resize', applyFixes);
            observer.disconnect();
            clearTimeout(timeoutId);
        };
    }, []);

    return <GlossaryProvider>{children}</GlossaryProvider>;
}
