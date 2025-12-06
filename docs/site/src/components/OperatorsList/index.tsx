// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React, { useState, useEffect, useMemo } from "react";
import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import useBaseUrl from "@docusaurus/useBaseUrl";
import styles from "./styles.module.css";

interface Operator {
    url: string;
    operator: string;
    cache?: boolean;
    functional?: boolean;
    type: "aggregator" | "publisher";
}

interface OperatorGroup {
    aggregators?: Record<string, { operator: string; cache?: boolean; functional?: boolean }>;
    publishers?: Record<string, { operator: string; cache?: boolean; functional?: boolean }>;
}

interface OperatorsData {
    mainnet: OperatorGroup;
    testnet: OperatorGroup;
}

const OperatorsList: React.FC = () => {
    const [operatorsData, setOperatorsData] = useState<OperatorsData | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [searchTerm, setSearchTerm] = useState("");
    const operatorsUrl = useBaseUrl("/operators.json");

    useEffect(() => {
        const fetchOperators = async () => {
            try {
                const response = await fetch(operatorsUrl);
                if (!response.ok) {
                    throw new Error("Failed to fetch operators data");
                }

                const data: OperatorsData = await response.json();
                setOperatorsData(data);
                setLoading(false);
            } catch (err) {
                setError(err instanceof Error ? err.message : "Unknown error occurred");
                setLoading(false);
            }
        };

        fetchOperators();
    }, []);

    const transformOperators = (group: OperatorGroup): Operator[] => {
        const operators: Operator[] = [];

        if (group.aggregators) {
            Object.entries(group.aggregators).forEach(([url, info]) => {
                operators.push({
                    url,
                    operator: info.operator,
                    cache: info.cache,
                    functional: info.functional,
                    type: "aggregator",
                });
            });
        }

        if (group.publishers) {
            Object.entries(group.publishers).forEach(([url, info]) => {
                operators.push({
                    url,
                    operator: info.operator,
                    cache: info.cache,
                    functional: info.functional,
                    type: "publisher",
                });
            });
        }

        return operators.sort((a, b) => a.operator.localeCompare(b.operator));
    };

    const filterOperators = (operators: Operator[], searchTerm: string): Operator[] => {
        if (!searchTerm.trim()) return operators;

        const lowercaseSearch = searchTerm.toLowerCase();
        return operators.filter(
            (op) =>
                op.operator.toLowerCase().includes(lowercaseSearch) ||
                op.url.toLowerCase().includes(lowercaseSearch) ||
                op.type.toLowerCase().includes(lowercaseSearch),
        );
    };

    const mainnetOperators = useMemo(() => {
        if (!operatorsData) return [];
        return transformOperators(operatorsData.mainnet);
    }, [operatorsData]);

    const testnetOperators = useMemo(() => {
        if (!operatorsData) return [];
        return transformOperators(operatorsData.testnet);
    }, [operatorsData]);

    const filteredMainnetOperators = useMemo(() => {
        return filterOperators(mainnetOperators, searchTerm);
    }, [mainnetOperators, searchTerm]);

    const filteredTestnetOperators = useMemo(() => {
        return filterOperators(testnetOperators, searchTerm);
    }, [testnetOperators, searchTerm]);

    const renderOperatorList = (operators: Operator[]) => {
        if (operators.length === 0) {
            return (
                <div className={styles.empty}>
                    {searchTerm
                        ? "No operators match your search criteria"
                        : "No operators available"}
                </div>
            );
        }

        return (
            <ul className={styles.operatorsList}>
                {operators.map((operator) => (
                    <li key={operator.url} className={styles.operatorItem}>
                        <div className={styles.operatorContent}>
                            <div className={styles.operatorHeader}>
                                <a
                                    href={operator.url}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className={styles.operatorUrl}
                                >
                                    {operator.url}
                                </a>
                                <span className={`${styles.operatorType} ${styles[operator.type]}`}>
                                    {operator.type}
                                </span>
                            </div>
                            <div className={styles.operatorDetails}>
                                <span className={styles.operatorName}>
                                    Operated by: {operator.operator}
                                </span>
                                <div className={styles.operatorFlags}>
                                    {operator.cache !== undefined && (
                                        <span
                                            className={`${styles.flag} ${operator.cache ? styles.positive : styles.negative}`}
                                        >
                                            Cache: {operator.cache ? "Yes" : "No"}
                                        </span>
                                    )}
                                    {operator.functional !== undefined && (
                                        <span
                                            className={`${styles.flag} ${operator.functional ? styles.positive : styles.negative}`}
                                        >
                                            Functional: {operator.functional ? "Yes" : "No"}
                                        </span>
                                    )}
                                </div>
                            </div>
                        </div>
                    </li>
                ))}
            </ul>
        );
    };

    if (loading) {
        return <div className={styles.loading}>Loading operators...</div>;
    }

    if (error) {
        return <div className={styles.error}>Error: {error}</div>;
    }

    return (
        <div className={styles.container}>
            <h3 className={styles.title}>Walrus Operators</h3>

            <div className={styles.searchContainer}>
                <input
                    type="text"
                    placeholder="Search by operator name, URL, or type..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    className={styles.searchInput}
                />
            </div>

            <Tabs>
                <TabItem value="mainnet" label="Mainnet" default>
                    <div className={styles.tabContent}>
                        <p className={styles.tabDescription}>
                            Mainnet operators ({filteredMainnetOperators.length} results)
                        </p>
                        {renderOperatorList(filteredMainnetOperators)}
                    </div>
                </TabItem>
                <TabItem value="testnet" label="Testnet">
                    <div className={styles.tabContent}>
                        <p className={styles.tabDescription}>
                            Testnet operators ({filteredTestnetOperators.length} results)
                        </p>
                        {renderOperatorList(filteredTestnetOperators)}
                    </div>
                </TabItem>
            </Tabs>
        </div>
    );
};

export default OperatorsList;
