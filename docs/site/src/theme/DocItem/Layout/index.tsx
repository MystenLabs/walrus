/*
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
*/

import React from "react";
import Layout from "@theme-original/DocItem/Layout";
import type LayoutType from "@theme/DocItem/Layout";
import type { WrapperProps } from "@docusaurus/types";

type Props = WrapperProps<typeof LayoutType>;

export default function DocItemLayoutWrapper(props: Props) {
  return (
    <>
      <Layout {...props} />
    </>
  );
}