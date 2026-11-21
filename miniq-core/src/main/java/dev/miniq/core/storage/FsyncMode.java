package dev.miniq.core.storage;

/** Durability policy defined by the v0.2 persistence specification. */
public enum FsyncMode {
    /** Complete writes to the operating system without forcing the file or directory. */
    NONE,
    /** Force each completed record and best-effort sync parent directories on creation. */
    EVERY_WRITE
}
