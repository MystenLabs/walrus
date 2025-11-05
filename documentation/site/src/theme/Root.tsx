import React from 'react';
import GlossaryProvider from '@site/src/components/Glossary/GlossaryProvider';

export default function Root({children}: {children: React.ReactNode}) {
  return <GlossaryProvider>{children}</GlossaryProvider>;
}