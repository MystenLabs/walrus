// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React, { useState, useEffect } from 'react';
import styles from './styles.module.css';

interface Portal {
  url: string;
  operator: string;
}

interface PortalsData {
  portals: Record<string, { operator: string }>;
}

const PortalsList: React.FC = () => {
  const [portals, setPortals] = useState<Portal[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchPortals = async () => {
    try {
        const response = await fetch('/portals.json');
        if (!response.ok) {
        throw new Error('Failed to fetch portals data');
        }

        const data: PortalsData = await response.json();

        // Transform the data into an array of Portal objects
        const portalsList: Portal[] = Object.entries(data.portals).map(([url, info]) => ({
        url,
        operator: info.operator,
        }));

        setPortals(portalsList);
        setLoading(false);
    } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error occurred');
        setLoading(false);
    }
    };

    fetchPortals();
  }, []);

  if (loading) {
    return <div className={styles.loading}>Loading portals...</div>;
  }

  if (error) {
    return <div className={styles.error}>Error: {error}</div>;
  }

  if (portals.length === 0) {
    return <div className={styles.empty}>No portals available</div>;
  }

  return (
    <div className={styles.container}>
    <h3 className={styles.title}>Available Portals</h3>
    <ul className={styles.portalsList}>
        {portals.map((portal) => (
        <li key={portal.url} className={styles.portalItem}>
            <div className={styles.portalContent}>
            <a
                href={portal.url}
                target="_blank"
                rel="noopener noreferrer"
                className={styles.portalUrl}
            >
                {portal.url}
            </a>
            <span className={styles.portalOperator}>
                Operated by: {portal.operator}
            </span>
            </div>
        </li>
        ))}
    </ul>
    </div>
  );
};

export default PortalsList;
