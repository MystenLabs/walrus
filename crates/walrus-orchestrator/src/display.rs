// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Display, io::stdout};

use crossterm::{
    cursor::{RestorePosition, SavePosition},
    style::{Print, PrintStyledContent, Stylize},
    terminal::{Clear, ClearType},
};
use prettytable::format::{self};

/// Print a bold green header message.
///
/// # Panics
///
/// Panics if writing to stdout fails.
pub fn header<S: Display>(message: S) {
    if cfg!(not(test)) {
        crossterm::execute!(
            stdout(),
            PrintStyledContent(format!("\n{message}\n").green().bold()),
        )
        .expect("Failed to print header to stdout");
    }
}

/// Print a bold red error message.
///
/// # Panics
///
/// Panics if writing to stdout fails.
pub fn error<S: Display>(message: S) {
    if cfg!(not(test)) {
        crossterm::execute!(
            stdout(),
            PrintStyledContent(format!("\n{message}\n").red().bold()),
        )
        .expect("Failed to print error to stdout");
    }
}

/// Print a bold warning message.
///
/// # Panics
///
/// Panics if writing to stdout fails.
pub fn warn<S: Display>(message: S) {
    if cfg!(not(test)) {
        crossterm::execute!(
            stdout(),
            PrintStyledContent(format!("\n{message}\n").bold()),
        )
        .expect("Failed to print warning to stdout");
    }
}

/// Print a configuration line as `name: value`.
///
/// # Panics
///
/// Panics if writing to stdout fails.
pub fn config<N: Display, V: Display>(name: N, value: V) {
    if cfg!(not(test)) {
        crossterm::execute!(
            stdout(),
            PrintStyledContent(format!("{name}: ").bold()),
            Print(format!("{value}\n"))
        )
        .expect("Failed to print config to stdout");
    }
}

/// Print an action message followed by ellipsis, and save cursor position.
///
/// # Panics
///
/// Panics if writing to stdout or saving cursor position fails.
pub fn action<S: Display>(message: S) {
    if cfg!(not(test)) {
        crossterm::execute!(stdout(), Print(format!("{message} ... ")), SavePosition)
            .expect("Failed to print action message to stdout");
    }
}

/// Print a status in square brackets at saved cursor position.
///
/// # Panics
///
/// Panics if writing to stdout or cursor operation fails.
pub fn status<S: Display>(status: S) {
    if cfg!(not(test)) {
        crossterm::execute!(
            stdout(),
            RestorePosition,
            SavePosition,
            Clear(ClearType::UntilNewLine),
            Print(format!("[{status}]"))
        )
        .expect("Failed to print status to stdout");
    }
}

/// Print a green "Ok" in square brackets and go to new line.
///
/// # Panics
///
/// Panics if writing to stdout or clearing terminal line fails.
pub fn done() {
    if cfg!(not(test)) {
        crossterm::execute!(
            stdout(),
            RestorePosition,
            Clear(ClearType::UntilNewLine),
            Print(format!("[{}]\n", "Ok".green()))
        )
        .expect("Failed to print done to stdout");
    }
}

/// Print a new line.
///
/// # Panics
///
/// Panics if writing to stdout fails.
pub fn newline() {
    if cfg!(not(test)) {
        crossterm::execute!(stdout(), Print("\n")).expect("Failed to print newline");
    }
}

/// Default style for tables printed to stdout.
pub fn default_table_format() -> format::TableFormat {
    format::FormatBuilder::new()
        .separators(
            &[
                format::LinePosition::Top,
                format::LinePosition::Bottom,
                format::LinePosition::Title,
            ],
            format::LineSeparator::new('-', '-', '-', '-'),
        )
        .padding(1, 1)
        .build()
}
