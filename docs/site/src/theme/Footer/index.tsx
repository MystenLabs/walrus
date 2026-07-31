// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import { useThemeConfig } from "@docusaurus/theme-common";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faGithub, faDiscord, faXTwitter } from "@fortawesome/free-brands-svg-icons";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Link from "@docusaurus/Link";

export default function Footer(): JSX.Element | null {
    const { footer } = useThemeConfig();
    if (!footer) {
        return null;
    }
    const { copyright } = footer;
    const { siteConfig } = useDocusaurusContext();
    const github = `https://github.com/${(siteConfig.customFields as any)?.github}`;
    const twitter = `https://x.com/${(siteConfig.customFields as any)?.twitterX}`;
    const discord = `http://discord.gg/${(siteConfig.customFields as any)?.discord}`;

    return (
        <footer className="py-6 border-t border-[#d5d4d1] dark:border-[#403f3e]">
            <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
                <div className="flex justify-between items-center flex-wrap gap-4">
                    {/* Left: text links first, then the copyright text */}
                    <div className="flex items-center flex-wrap gap-x-8 gap-y-2 text-sm">
                        <div className="flex items-center gap-x-2 !text-black dark:!text-white">
                            <Link
                                className="!text-black dark:!text-white hover:opacity-70 transition-opacity"
                                to="/docs/legal/privacy"
                            >
                                Privacy
                            </Link>
                            <span aria-hidden="true">&bull;</span>
                            <Link
                                className="!text-black dark:!text-white hover:opacity-70 transition-opacity"
                                to="/docs/legal/walrus_general_tos"
                            >
                                TOS
                            </Link>
                            <span aria-hidden="true">&bull;</span>
                            <Link
                                className="!text-black dark:!text-white hover:opacity-70 transition-opacity"
                                to="/docs/legal/testnet_tos"
                            >
                                Testnet TOS
                            </Link>
                        </div>
                        {copyright && (
                            <span className="!text-[#878683] dark:!text-[#b0afac]">{copyright}</span>
                        )}
                    </div>

                    {/* Right: social media icons */}
                    <div className="flex gap-4">
                        <a
                            href={discord}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="!text-black dark:!text-white hover:opacity-70 transition-opacity duration-200 flex items-center"
                            aria-label="Join us on Discord"
                        >
                            <FontAwesomeIcon icon={faDiscord} size="lg" />
                        </a>

                        <a
                            href={twitter}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="!text-black dark:!text-white hover:opacity-70 transition-opacity duration-200 flex items-center"
                            aria-label="Follow us on Twitter"
                        >
                            <FontAwesomeIcon icon={faXTwitter} size="lg" />
                        </a>

                        <a
                            href={github}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="!text-black dark:!text-white hover:opacity-70 transition-opacity duration-200 flex items-center"
                            aria-label="View source on GitHub"
                        >
                            <FontAwesomeIcon icon={faGithub} size="lg" />
                        </a>
                    </div>
                </div>
            </div>
        </footer>
    );
}
