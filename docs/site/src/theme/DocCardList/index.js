// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React from 'react';
import DocCardList from '@theme-original/DocCardList';
import { useCurrentSidebarCategory } from '@docusaurus/theme-common';

export default function DocCardListWrapper(props) {
  if (props.items) {
    return <DocCardList {...props} />;
  }

  try {
    const category = useCurrentSidebarCategory();
    return <DocCardList items={category.items} {...props} />;
  } catch {
    return null;
  }
}
