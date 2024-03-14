// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Merkle tree implementation for Walrus.
use std::{fmt::Debug, marker::PhantomData};

use fastcrypto::hash::{Blake2b256, Digest, HashFunction};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The length of the digests used in the merkle tree.
pub const DIGEST_LEN: usize = 32;

const LEAF_PREFIX: [u8; 1] = [0];
const INNER_PREFIX: [u8; 1] = [1];
const EMPTY_NODE: [u8; DIGEST_LEN] = [0; DIGEST_LEN];

/// Returned if the specified index is out of bounds for a Merkle tree or proof.
#[derive(Error, Debug, PartialEq, Eq)]
#[error("index {0} is too large")]
pub struct LeafIndexOutOfBounds(usize);

/// A node in the Merkle tree
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub enum Node {
    /// A node with an empty subtree.
    Empty,
    /// A node with children, hash value of length 32 bytes.
    Digest([u8; DIGEST_LEN]),
}

impl Node {
    /// Get the byte representation of [`self`].
    pub fn bytes(&self) -> [u8; DIGEST_LEN] {
        match self {
            Node::Empty => EMPTY_NODE,
            Node::Digest(val) => *val,
        }
    }
}

impl From<Digest<DIGEST_LEN>> for Node {
    fn from(value: Digest<DIGEST_LEN>) -> Self {
        Self::Digest(value.digest)
    }
}

impl From<[u8; DIGEST_LEN]> for Node {
    fn from(value: [u8; DIGEST_LEN]) -> Self {
        Self::Digest(value)
    }
}

impl AsRef<[u8]> for Node {
    fn as_ref(&self) -> &[u8] {
        match self {
            Node::Empty => EMPTY_NODE.as_ref(),
            Node::Digest(val) => val.as_ref(),
        }
    }
}

/// The operations required to authenticate a Merkle proof.
pub trait MerkleAuth {
    /// Verify the proof given a Merkle root and the leaf data.
    fn verify_proof(&self, root: &Node, leaf: &[u8]) -> bool {
        self.compute_root(leaf) == *root
    }
    /// Recompute the Merkle root from the proof and the provided leaf data.
    fn compute_root(&self, leaf: &[u8]) -> Node;
}

/// A proof that some data is at index `leaf_index` in a [`MerkleTree`].
#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct MerkleProof<T = Blake2b256> {
    _hash_type: PhantomData<T>,
    /// The sibling hash values on the path from the leaf to the root.
    path: Vec<Node>,
    /// The index of the leaf in the committed vector.
    leaf_index: usize,
}

impl<T> MerkleProof<T>
where
    T: HashFunction<DIGEST_LEN>,
{
    /// Construct Merkle proof from list of hashes and leaf index.
    pub fn new(path: &[Node], leaf_index: usize) -> Result<Self, LeafIndexOutOfBounds> {
        if leaf_index >> path.len() != 0 {
            return Err(LeafIndexOutOfBounds(leaf_index));
        }
        Ok(Self {
            _hash_type: PhantomData,
            path: path.into(),
            leaf_index,
        })
    }
}

impl<T> MerkleAuth for MerkleProof<T>
where
    T: HashFunction<DIGEST_LEN>,
{
    fn compute_root(&self, leaf: &[u8]) -> Node {
        let mut current_hash = leaf_hash::<T>(leaf);
        let mut level_idx = self.leaf_index;
        for sibling in self.path.iter() {
            // The sibling hash of the current node
            if level_idx % 2 == 0 {
                // The current node is a left child
                current_hash = inner_hash::<T>(&current_hash, sibling);
            } else {
                // The current node is a right child
                current_hash = inner_hash::<T>(sibling, &current_hash);
            };
            // Update to the level index one level up in the tree
            level_idx /= 2;
        }
        current_hash
    }
}

/// Merkle tree using a hash function `T` (default: [`Blake2b256`]) from the [`fastcrypto`] crate.
///
/// The data of the leaves is prefixed with `0x00` before hashing and hashes of inner nodes are
/// computed over the concatenation of their children prefixed with `0x01`. Hashes of empty
/// subtrees (i.e. subtrees without data at their leaves) are replaced with all zeros.
#[derive(Serialize, Deserialize)]
pub struct MerkleTree<T = Blake2b256> {
    _hash_type: PhantomData<T>,
    // The nodes of the Merkle tree are stored in a vector level by level starting with
    // the leaf hashes.
    nodes: Vec<Node>,
    n_leaves: usize,
}

impl<T> MerkleTree<T>
where
    T: HashFunction<DIGEST_LEN>,
{
    /// Create the [`MerkleTree`] as a commitment to the provided data.
    pub fn build<I>(iter: I) -> Self
    where
        I: IntoIterator,
        I::IntoIter: ExactSizeIterator,
        I::Item: AsRef<[u8]>,
    {
        let iter = iter.into_iter();

        // Create the capacity that we know will be needed, since the vec will be
        // reused by the call to from_leaf_nodes.
        let mut nodes = Vec::with_capacity(n_nodes(iter.len()));

        // Hash each leaf prefixed with `LEAF_PREFIX` and insert the hash into `nodes`
        nodes.extend(iter.map(|leaf| leaf_hash::<T>(leaf.as_ref())));

        let n_leaves = nodes.len();
        let mut level_nodes = n_leaves;
        let mut prev_level_index = 0;

        // Fill all other nodes of the Merkle Tree
        while level_nodes > 1 {
            if level_nodes % 2 == 1 {
                // We need an empty sibling for the last node on the previous level
                nodes.push(Node::Empty);
                level_nodes += 1;
            }

            let new_level_index = prev_level_index + level_nodes;

            (prev_level_index..new_level_index)
                .step_by(2)
                .for_each(|index| nodes.push(inner_hash::<T>(&nodes[index], &nodes[index + 1])));

            prev_level_index = new_level_index;
            level_nodes /= 2;
        }

        Self {
            nodes,
            n_leaves,
            _hash_type: PhantomData,
        }
    }

    /// Verify that the root of `self` matches the provided root hash.
    pub fn verify_root(&self, root: &Node) -> bool {
        self.root() == *root
    }

    /// Get a copy of the root hash of `self`.
    pub fn root(&self) -> Node {
        self.nodes.last().map_or(Node::Empty, |val| val.clone())
    }

    /// Get the [`MerkleProof`] for the leaf at `leaf_index` consisting
    /// of all sibling hashes on the path from the leaf to the root.
    pub fn get_proof(&self, leaf_index: usize) -> Result<MerkleProof<T>, LeafIndexOutOfBounds> {
        if leaf_index >= self.n_leaves {
            return Err(LeafIndexOutOfBounds(leaf_index));
        }
        let mut path = Vec::with_capacity(self.n_leaves.ilog2() as usize + 1);
        let mut level_idx = leaf_index;
        let mut n_level = self.n_leaves;
        let mut level_base_idx = 0;
        while n_level > 1 {
            // All levels contain an even number of nodes
            n_level = n_level.next_multiple_of(2);
            let sibling_idx = if level_idx % 2 == 0 {
                level_base_idx + level_idx + 1
            } else {
                level_base_idx + level_idx - 1
            };
            path.push(self.nodes[sibling_idx].clone());
            // Index of the parent on the next level
            level_idx /= 2;
            level_base_idx += n_level;
            n_level /= 2;
        }
        Ok(MerkleProof {
            _hash_type: PhantomData,
            path,
            leaf_index,
        })
    }
}

fn leaf_hash<T>(input: &[u8]) -> Node
where
    T: HashFunction<DIGEST_LEN>,
{
    let mut hash_fun = T::default();
    hash_fun.update(LEAF_PREFIX);
    hash_fun.update(input);
    hash_fun.finalize().into()
}

fn inner_hash<T>(left: &Node, right: &Node) -> Node
where
    T: HashFunction<DIGEST_LEN>,
{
    let mut hash_fun = T::default();
    hash_fun.update(INNER_PREFIX);
    hash_fun.update(left.bytes());
    hash_fun.update(right.bytes());
    hash_fun.finalize().into()
}

fn n_nodes(n_leaves: usize) -> usize {
    let mut lvl_nodes = n_leaves;
    let mut tot_nodes = 0;
    while lvl_nodes > 1 {
        lvl_nodes += lvl_nodes % 2;
        tot_nodes += lvl_nodes;
        lvl_nodes /= 2;
    }
    tot_nodes + lvl_nodes
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_n_nodes() {
        assert!(n_nodes(0) == 0);
        assert!(n_nodes(1) == 1);
        assert!(n_nodes(2) == 3);
        assert!(n_nodes(3) == 7);
        assert!(n_nodes(4) == 7);
        assert!(n_nodes(5) == 13);
        assert!(n_nodes(6) == 13);
        assert!(n_nodes(7) == 15);
        assert!(n_nodes(8) == 15);
        assert!(n_nodes(9) == 23);
    }

    #[test]
    fn test_merkle_tree_empty() {
        let mt: MerkleTree = MerkleTree::build::<[&[u8]; 0]>([]);
        assert_eq!(mt.root().bytes(), EMPTY_NODE);
    }

    #[test]
    fn test_merkle_tree_single_element() {
        let test_inp = "Test";
        let mt: MerkleTree = MerkleTree::build(&[test_inp.as_bytes()]);
        let mut hash_fun = Blake2b256::default();
        hash_fun.update(LEAF_PREFIX);
        hash_fun.update(test_inp.as_bytes());
        assert_eq!(mt.root().bytes(), hash_fun.finalize().digest);
    }

    #[test]
    fn test_merkle_tree_empty_element() {
        let mt: MerkleTree = MerkleTree::build(&[&[]]);
        let mut hash_fun = Blake2b256::default();
        hash_fun.update(LEAF_PREFIX);
        hash_fun.update([]);
        assert_eq!(mt.root().bytes(), hash_fun.finalize().digest);
    }

    #[test]
    fn test_get_path_out_of_bounds() {
        let test_inp: Vec<_> = [
            "foo", "bar", "fizz", "baz", "buzz", "fizz", "foobar", "walrus", "fizz",
        ]
        .iter()
        .map(|x| x.as_bytes())
        .collect();
        for i in 0..test_inp.len() {
            let mt: MerkleTree = MerkleTree::build(&test_inp[..i]);
            match mt.get_proof(i.next_power_of_two()) {
                Err(_) => {}
                Ok(_) => panic!("Expected an error"),
            }
        }
    }

    #[test]
    fn test_merkle_path_verify() {
        let test_inp: Vec<_> = [
            "foo", "bar", "fizz", "baz", "buzz", "fizz", "foobar", "walrus", "fizz",
        ]
        .iter()
        .map(|x| x.as_bytes())
        .collect();
        for i in 0..test_inp.len() {
            let mt: MerkleTree = MerkleTree::build(&test_inp[..i]);
            for (idx, leaf_data) in test_inp[..i].iter().enumerate() {
                let proof = mt.get_proof(idx).unwrap();
                assert!(proof.verify_proof(&mt.root(), leaf_data));
            }
        }
    }
}
