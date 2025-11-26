import { useThemeConfig } from '@docusaurus/theme-common';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faGithub, faDiscord, faXTwitter } from '@fortawesome/free-brands-svg-icons';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Link from '@docusaurus/Link';

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
    <footer className="bg-wal-purple-darker py-6 border-t border-wal-green-dark">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center flex-wrap gap-2">
          {copyright && (
            <span className="text-wal-white text-sm">
              {copyright}
            </span>
          )}
          <div className='text-wal-white-70'>
            <Link className='text-wal-white-70 hover:text-wal-white-100' to="/docs/legal/privacy">Privacy</Link> &bull; <Link className='text-wal-white-70 hover:text-wal-white-100' to="/docs/legal/walrus_general_tos">TOS</Link> &bull; <Link className='text-wal-white-70 hover:text-wal-white-100' to="/docs/legal/testnet_tos">Tesnet TOS</Link>
          </div>
          <div className="flex gap-4">
          <a 
            href={discord}
            target="_blank"
            rel="noopener noreferrer"
            className="!text-wal-white-70 hover:!text-wal-white-100 transition-colors duration-200 flex items-center mr-1"
            aria-label="Join us on Discord"
          >
            <FontAwesomeIcon icon={faDiscord} size="lg" />
          </a>

          <a 
            href={twitter}
            target="_blank"
            rel="noopener noreferrer"
            className="!text-wal-white-70 hover:!text-wal-white-100 transition-colors duration-200 flex items-center mr-1"
            aria-label="Follow us on Twitter"
          >
            <FontAwesomeIcon icon={faXTwitter} size="lg" />
          </a>

          <a 
            href={github}
            target="_blank"
            rel="noopener noreferrer"
            className="!text-wal-white-70 hover:!text-wal-white-100 transition-colors duration-200 flex items-center"
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
