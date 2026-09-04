> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

{() => {
    const style = document.createElement('style');
    style.textContent = `
      .docMainContainer_t2hy, [class*="docMainContainer"] { max-width: 100% !important; }
      .container { max-width: 100% !important; padding: 0 8px !important; }
      .col { max-width: 100% !important; flex: 0 0 100% !important; padding: 0 !important; }
      [class*="docItemContainer"] { max-width: 100% !important; padding: 0 !important; }
      .theme-doc-breadcrumbs, .pagination-nav, article > header,
      .theme-doc-toc-desktop, .theme-doc-toc-mobile,
      [class*="copyPageButton"], .theme-doc-markdown > p,
      .theme-doc-markdown > h1, .theme-doc-markdown > header,
      [class*="docTitle"], [class*="docFooter"],
      footer.py-6, .theme-doc-footer { display: none !important; }
      .theme-doc-markdown { margin: 0 !important; }
      .padding-top--md { padding-top: 0 !important; }
      .padding-bottom--lg { padding-bottom: 0 !important; }
    `;
    document.head.appendChild(style);
    return null;
  }}

  {() => {
    const [failed, setFailed] = React.useState(false);

    if (failed) {
      return (
        
          The interactive API reference cannot load in this environment.
          [Open the API reference in a new tab](https://oyster.testnet.mystenlabs.com/api/docs)
        
      );
    }

    return (
      <iframe
        src="https://oyster.testnet.mystenlabs.com/api/docs"
        style={{
          width: '100%',
          height: 'calc(100vh - 120px)',
          border: 'none',
          display: 'block',
        }}
        title="Walrus Oyster API Reference"
        onError={() => setFailed(true)}
        onLoad={(e) => {
          try {
            const doc = e.target.contentDocument;
            if (!doc || !doc.body || doc.body.innerHTML === '') setFailed(true);
          } catch (_) {
            // Cross-origin iframe loaded successfully
          }
        }}
      />
    );
  }}