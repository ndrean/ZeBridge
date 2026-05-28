// SPDX-FileCopyrightText: Copyright (c) 2025 g41797
// SPDX-License-Identifier: MIT

pub fn MailBox(comptime Letter: type) type {
    return struct {
        const Self = @This();

        /// Envelope inside FIFO wrapping the actual letter.
        pub const Envelope = struct {
            prev: ?*Envelope = null,
            next: ?*Envelope = null,
            letter: Letter,
        };

        first: ?*Envelope = null,
        last: ?*Envelope = null,
        len: usize = 0,
        closed: bool = false,
        mutex: Mutex = .{ .state = std.atomic.Value(Mutex.State).init(.unlocked) },
        cond: Condition = Condition.init,
        interrupted: bool = false,

        /// Append a new Envelope to the tail
        /// and wake-up waiting on receive threads.
        /// Arguments:
        ///     new_Envelope: Pointer to the new Envelope to append.
        /// If mailbox was closed - returns error.Closed
        pub fn send(mbox: *Self, new_Envelope: *Envelope, io: std.Io) error{Closed}!void {
            mbox.mutex.lockUncancelable(io);
            defer mbox.mutex.unlock(io);

            if (mbox.closed) {
                return error.Closed;
            }

            mbox.enqueue(new_Envelope);

            mbox.cond.signal(io);
        }

        /// Wake-up waiting on receive thread.
        /// If mailbox was closed - returns error.Closed
        /// If waiting was already interrupted -  - returns error.AlreadyInterrupted
        pub fn interrupt(mbox: *Self, io: std.Io) error{ Closed, AlreadyInterrupted }!void {
            mbox.mutex.lockUncancelable(io);
            defer mbox.mutex.unlock(io);

            if (mbox.closed) {
                return error.Closed;
            }

            if (mbox.interrupted) {
                return error.AlreadyInterrupted;
            }

            mbox.interrupted = true;

            mbox.cond.signal(io);
        }

        /// Blocks thread  maximum timeout_ns till Envelope in head of FIFO will be available.
        /// If not available - returns error.Timeout.
        /// Otherwise removes Envelope from the head and returns it to the caller.
        /// If mailbox was closed - returns error.Closed
        /// If interrupt was issued - returns error.Interrupted
        pub fn receive(mbox: *Self, timeout_ns: u64, io: std.Io) error{ Timeout, Closed, Interrupted }!*Envelope {
            const timeout_start = nanoNow();

            mbox.mutex.lockUncancelable(io);
            defer mbox.mutex.unlock(io);

            while (mbox.len == 0) {
                if (mbox.closed) {
                    return error.Closed;
                }

                if (mbox.interrupted) {
                    mbox.interrupted = false;
                    return error.Interrupted;
                }

                const elapsed = nanoNow() - timeout_start;
                if (elapsed > timeout_ns)
                    return error.Timeout;

                const local_timeout_ns = timeout_ns - elapsed;
                mbox.mutex.unlock(io);
                sleep(@min(local_timeout_ns, std.time.ns_per_ms));
                mbox.mutex.lockUncancelable(io);
            }

            if (mbox.closed) {
                return error.Closed;
            }

            if (mbox.interrupted) {
                mbox.interrupted = false;
                return error.Interrupted;
            }

            const first = mbox.dequeue();

            if (first) |firstEnvelope| {
                defer mbox.cond.signal(io);
                return firstEnvelope;
            } else {
                return error.Timeout;
            }
        }

        /// # of letters in internal queue.
        /// May be called also on closed mailbox.
        pub fn letters(mbox: *Self, io: std.Io) usize {
            mbox.mutex.lockUncancelable(io);
            defer mbox.mutex.unlock(io);

            return mbox.len;
        }

        /// First close disabled further client calls and returns head of Envelopes
        /// for de-allocation
        pub fn close(mbox: *Self, io: std.Io) ?*Envelope {
            mbox.mutex.lockUncancelable(io);
            defer mbox.mutex.unlock(io);

            if (mbox.closed) return null;

            mbox.closed = true;
            mbox.interrupted = false;

            const head = mbox.first;

            mbox.first = null;

            mbox.cond.signal(io);

            return head;
        }

        fn enqueue(fifo: *Self, new_Envelope: *Envelope) void {
            new_Envelope.prev = null;
            new_Envelope.next = null;

            if (fifo.len == 0) {
                std.debug.assert(fifo.first == null);
                std.debug.assert(fifo.last == null);
                fifo.first = new_Envelope;
            }

            if (fifo.last) |last| {
                last.next = new_Envelope;
                new_Envelope.prev = last;
            } else {
                fifo.first = new_Envelope;
            }

            fifo.last = new_Envelope;
            fifo.len += 1;

            return;
        }

        fn dequeue(fifo: *Self) ?*Envelope {
            if (fifo.len == 0) {
                return null;
            }

            var result = fifo.first;
            fifo.first = result.?.next;

            if (fifo.len == 1) {
                fifo.last = null;
            } else {
                fifo.first.?.prev = fifo.first;
            }

            result.?.prev = null;
            result.?.next = null;
            fifo.len -= 1;

            return result;
        }
    };
}

pub fn MailBoxIntrusive(comptime Envelope: type) type {
    return struct {
        const Self = @This();

        /// Envelope has following structure
        /// in order to be intrusive
        /// pub const T = struct {
        ///     prev: ?*T = null,
        ///     next: ?*T = null,
        ///     additional stuff
        /// };
        first: ?*Envelope = null,
        last: ?*Envelope = null,
        len: usize = 0,
        closed: bool = false,
        mutex: Mutex = .{ .state = std.atomic.Value(Mutex.State).init(.unlocked) },
        cond: Condition = Condition.init,
        interrupted: bool = false,

        /// Append a new Envelope to the tail
        /// and wake-up waiting on receive threads.
        /// Arguments:
        ///     new_Envelope: Pointer to the new Envelope to append.
        /// If mailbox was closed - returns error.Closed
        pub fn send(mbox: *Self, new_Envelope: *Envelope, io: std.Io) error{Closed}!void {
            mbox.mutex.lockUncancelable(io);
            defer mbox.mutex.unlock(io);

            if (mbox.closed) {
                return error.Closed;
            }

            mbox.enqueue(new_Envelope);

            mbox.cond.signal(io);
        }

        /// Wake-up waiting on receive thread.
        /// If mailbox was closed - returns error.Closed
        /// If waiting was already interrupted -  - returns error.AlreadyInterrupted
        pub fn interrupt(mbox: *Self, io: std.Io) error{ Closed, AlreadyInterrupted }!void {
            mbox.mutex.lockUncancelable(io);
            defer mbox.mutex.unlock(io);

            if (mbox.closed) {
                return error.Closed;
            }

            if (mbox.interrupted) {
                return error.AlreadyInterrupted;
            }

            mbox.interrupted = true;

            mbox.cond.signal(io);
        }

        /// Blocks thread  maximum timeout_ns till Envelope in head of FIFO will be available.
        /// If not available - returns error.Timeout.
        /// Otherwise removes Envelope from the head and returns it to the caller.
        /// If mailbox was closed - returns error.Closed
        /// If interrupt was issued - returns error.Interrupted
        pub fn receive(mbox: *Self, timeout_ns: u64, io: std.Io) error{ Timeout, Closed, Interrupted }!*Envelope {
            const timeout_start = nanoNow();

            mbox.mutex.lockUncancelable(io);
            defer mbox.mutex.unlock(io);

            while (mbox.len == 0) {
                if (mbox.closed) {
                    return error.Closed;
                }

                if (mbox.interrupted) {
                    mbox.interrupted = false;
                    return error.Interrupted;
                }

                const elapsed = nanoNow() - timeout_start;
                if (elapsed > timeout_ns)
                    return error.Timeout;

                const local_timeout_ns = timeout_ns - elapsed;
                mbox.mutex.unlock(io);
                sleep(@min(local_timeout_ns, std.time.ns_per_ms));
                mbox.mutex.lockUncancelable(io);
            }

            if (mbox.closed) {
                return error.Closed;
            }

            if (mbox.interrupted) {
                mbox.interrupted = false;
                return error.Interrupted;
            }

            const first = mbox.dequeue();

            if (first) |firstEnvelope| {
                defer mbox.cond.signal(io);
                return firstEnvelope;
            } else {
                return error.Timeout;
            }
        }

        /// # of letters in internal queue.
        /// May be called also on closed mailbox.
        pub fn letters(mbox: *Self, io: std.Io) usize {
            mbox.mutex.lockUncancelable(io);
            defer mbox.mutex.unlock(io);

            return mbox.len;
        }

        /// First close disabled further client calls and returns head of Envelopes
        /// for de-allocation
        pub fn close(mbox: *Self, io: std.Io) ?*Envelope {
            mbox.mutex.lockUncancelable(io);
            defer mbox.mutex.unlock(io);

            if (mbox.closed) return null;

            mbox.closed = true;

            const head = mbox.first;

            mbox.first = null;

            mbox.cond.signal(io);

            return head;
        }

        fn enqueue(fifo: *Self, new_Envelope: *Envelope) void {
            new_Envelope.prev = null;
            new_Envelope.next = null;

            if (fifo.len == 0) {
                std.debug.assert(fifo.first == null);
                std.debug.assert(fifo.last == null);
                fifo.first = new_Envelope;
            }

            if (fifo.last) |last| {
                last.next = new_Envelope;
                new_Envelope.prev = last;
            } else {
                fifo.first = new_Envelope;
            }

            fifo.last = new_Envelope;
            fifo.len += 1;

            return;
        }

        fn dequeue(fifo: *Self) ?*Envelope {
            if (fifo.len == 0) {
                return null;
            }

            var result = fifo.first;
            fifo.first = result.?.next;

            if (fifo.len == 1) {
                fifo.last = null;
            } else {
                fifo.first.?.prev = fifo.first;
            }

            result.?.prev = null;
            result.?.next = null;
            fifo.len -= 1;

            return result;
        }
    };
}

/// Intrusive, type-erased mailbox based on std.DoublyLinkedList.
/// The stored objects must embed `std.DoublyLinkedList.Node`.
/// const Envelope = struct {
///     // user data
///     node: std.DoublyLinkedList.Node,
/// };
pub const TypeErasedMailbox = struct {
    const Self = @This();

    pub const Node = std.DoublyLinkedList.Node;
    pub const List = std.DoublyLinkedList;

    list: List = .{
        .first = null,
        .last = null,
    },

    len: usize = 0, // tracking the length separately according to recommendation
    closed: bool = false,
    interrupted: bool = false,

    mutex: Mutex = .{ .state = std.atomic.Value(Mutex.State).init(.unlocked) },
    cond: Condition = .{ .state = std.atomic.Value(Condition.State).init(0), .epoch = 0 },

    /// Append a node to the tail of the mailbox.
    /// Fails if mailbox is closed.
    pub fn send(mbox: *Self, node: *Node, io: std.Io) error{Closed}!void {
        mbox.mutex.lockUncancelable(io);
        defer mbox.mutex.unlock(io);

        if (mbox.closed)
            return error.Closed;

        mbox.list.append(node);
        mbox.len += 1;

        mbox.cond.signal(io);
    }

    /// Interrupt a waiting receiver.
    pub fn interrupt(mbox: *Self, io: std.Io) error{ Closed, AlreadyInterrupted }!void {
        mbox.mutex.lockUncancelable(io);
        defer mbox.mutex.unlock(io);

        if (mbox.closed)
            return error.Closed;

        if (mbox.interrupted)
            return error.AlreadyInterrupted;

        mbox.interrupted = true;
        mbox.cond.signal(io);
    }

    /// Receive a node from the head of the mailbox.
    /// Blocks up to `timeout_ns`.
    pub fn receive(
        mbox: *Self,
        timeout_ns: u64,
        io: std.Io,
    ) error{ Timeout, Closed, Interrupted }!*Node {
        const timer_start = nanoNow();

        mbox.mutex.lockUncancelable(io);
        defer mbox.mutex.unlock(io);

        while (mbox.len == 0) {
            if (mbox.closed)
                return error.Closed;

            if (mbox.interrupted) {
                mbox.interrupted = false;
                return error.Interrupted;
            }

            const elapsed = nanoNow() - timer_start;
            if (elapsed >= timeout_ns)
                return error.Timeout;

            const local_timeout_ns = timeout_ns - elapsed;
            mbox.mutex.unlock(io);
            sleep(@min(local_timeout_ns, std.time.ns_per_ms));
            mbox.mutex.lockUncancelable(io);
        }

        if (mbox.closed) {
            return error.Closed;
        }

        if (mbox.interrupted) {
            mbox.interrupted = false;
            return error.Interrupted;
        }

        const node = mbox.list.popFirst().?;
        mbox.len -= 1;

        mbox.cond.signal(io);
        return node;
    }

    /// Number of queued items.
    pub fn letters(mbox: *Self, io: std.Io) usize {
        mbox.mutex.lockUncancelable(io);
        defer mbox.mutex.unlock(io);
        return mbox.len;
    }

    /// Close mailbox.
    /// Returns the head node of the remaining list (caller cleans).
    pub fn close(mbox: *Self, io: std.Io) ?*Node {
        mbox.mutex.lockUncancelable(io);
        defer mbox.mutex.unlock(io);

        if (mbox.closed)
            return null;

        mbox.closed = true;
        mbox.interrupted = false;

        const head = mbox.list.first;
        mbox.list = .{};
        mbox.len = 0;

        mbox.cond.signal(io);
        return head;
    }
};

const std = @import("std");
const Mutex = std.Io.Mutex;
const Condition = std.Io.Condition;

const c_time = @cImport(@cInclude("time.h"));

fn sleep(ns: u64) void {
    var ts = c_time.struct_timespec{
        .tv_sec = @as(c_time.time_t, @intCast(ns / std.time.ns_per_s)),
        .tv_nsec = @as(c_long, @intCast(ns % std.time.ns_per_s)),
    };
    _ = c_time.nanosleep(&ts, null);
}

fn nanoNow() u64 {
    var ts: c_time.struct_timespec = undefined;
    _ = c_time.clock_gettime(c_time.CLOCK_MONOTONIC, &ts);
    return @as(u64, @intCast(ts.tv_sec)) * std.time.ns_per_s +
        @as(u64, @intCast(ts.tv_nsec));
}
