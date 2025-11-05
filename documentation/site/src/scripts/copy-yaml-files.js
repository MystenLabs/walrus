#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

// Define source and destination directories
const dataDir = path.join(__dirname, '../../../data');
const staticDir = path.join(__dirname, '../../static');

// Ensure static directory exists
if (!fs.existsSync(staticDir)) {
  fs.mkdirSync(staticDir, { recursive: true });
}

// Get all YAML files from data directory
function getYamlFiles(dir) {
  const files = [];
  const items = fs.readdirSync(dir);
  
  for (const item of items) {
    const fullPath = path.join(dir, item);
    const stat = fs.statSync(fullPath);
    
    if (stat.isDirectory()) {
      // Recursively get YAML files from subdirectories
      files.push(...getYamlFiles(fullPath));
    } else if (item.endsWith('.yaml') || item.endsWith('.yml')) {
      files.push(fullPath);
    }
  }
  
  return files;
}

// Copy YAML files to static directory
function copyYamlFiles() {
  try {
    const yamlFiles = getYamlFiles(dataDir);
    
    if (yamlFiles.length === 0) {
      console.log('No YAML files found in data directory');
      return;
    }
    
    console.log(`Found ${yamlFiles.length} YAML file(s):`);
    
    for (const yamlFile of yamlFiles) {
      const relativePath = path.relative(dataDir, yamlFile);
      const destPath = path.join(staticDir, relativePath);
      const destDir = path.dirname(destPath);
      
      // Ensure destination directory exists
      if (!fs.existsSync(destDir)) {
        fs.mkdirSync(destDir, { recursive: true });
      }
      
      // Copy file
      fs.copyFileSync(yamlFile, destPath);
      console.log(`  Copied: ${relativePath} -> static/${relativePath}`);
    }
    
    console.log('All YAML files copied successfully!');
  } catch (error) {
    console.error('Error copying YAML files:', error.message);
    process.exit(1);
  }
}

// Run the script
copyYamlFiles();