#!/usr/bin/env python3
import socket
import argparse
import time
from typing import List, Dict, Any, Tuple, Optional, Union


class RESPEncoder:
    @staticmethod
    def encode_command(command: List[str]) -> bytes:
        """Encode a command as a RESP array."""
        parts = [f"*{len(command)}\r\n"]
        for arg in command:
            parts.append(f"${len(arg)}\r\n{arg}\r\n")
        return "".join(parts).encode("utf-8")


class RESPDecoder:
    @staticmethod
    def decode_response(data: bytes) -> Tuple[Any, int]:
        """Decode a RESP response."""
        if not data:
            return None, 0

        data_str = data.decode("utf-8")
        data_type = data_str[0]

        if data_type == "+":  # Simple string
            end_idx = data_str.find("\r\n")
            return data_str[1:end_idx], end_idx + 2

        elif data_type == "-":  # Error
            end_idx = data_str.find("\r\n")
            return f"ERROR: {data_str[1:end_idx]}", end_idx + 2

        elif data_type == ":":  # Integer
            end_idx = data_str.find("\r\n")
            return int(data_str[1:end_idx]), end_idx + 2

        elif data_type == "$":  # Bulk string
            length_end = data_str.find("\r\n")
            length = int(data_str[1:length_end])

            if length == -1:  # Null bulk string
                return None, length_end + 2

            start_idx = length_end + 2
            end_idx = start_idx + length
            return data_str[start_idx:end_idx], end_idx + 2

        elif data_type == "*":  # Array
            length_end = data_str.find("\r\n")
            count = int(data_str[1:length_end])

            if count == -1:  # Null array
                return None, length_end + 2

            result = []
            offset = length_end + 2

            for _ in range(count):
                element, bytes_read = RESPDecoder.decode_response(data[offset:])
                result.append(element)
                offset += bytes_read

            return result, offset

        raise ValueError(f"Unknown RESP data type: {data_type}")


class RedisClient:
    def __init__(self, host: str = "localhost", port: int = 6379):
        """Initialize Redis client with server connection details."""
        self.host = host
        self.port = port
        self.socket = None
        self.connected = False

    def connect(self) -> bool:
        """Connect to the Redis server."""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.connected = True
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            self.connected = False
            return False

    def disconnect(self) -> None:
        """Disconnect from the Redis server."""
        if self.socket:
            self.socket.close()
        self.connected = False

    def execute_command(self, *args) -> Any:
        """Execute a Redis command and return the response."""
        if not self.connected:
            if not self.connect():
                return "Not connected to server"

        try:
            # Encode command
            command = [str(arg) for arg in args]
            encoded_command = RESPEncoder.encode_command(command)

            # Send command
            self.socket.sendall(encoded_command)

            # Receive response (simple approach - doesn't handle very large responses)
            response = self.socket.recv(4096)
            result, _ = RESPDecoder.decode_response(response)
            return result

        except Exception as e:
            print(f"Error executing command: {e}")
            self.disconnect()
            return f"Error: {e}"

    # Basic Redis commands
    def ping(self) -> str:
        """Send PING command to server."""
        return self.execute_command("PING")

    def echo(self, message: str) -> str:
        """Send ECHO command to server."""
        return self.execute_command("ECHO", message)

    def set(self, key: str, value: str, px: Optional[int] = None) -> str:
        """Set a key-value pair, with optional expiration in milliseconds."""
        if px is not None:
            return self.execute_command("SET", key, value, "PX", px)
        return self.execute_command("SET", key, value)

    def get(self, key: str) -> str:
        """Get value for a key."""
        return self.execute_command("GET", key)

    def incr(self, key: str) -> int:
        """Increment the value of a key."""
        return self.execute_command("INCR", key)

    def keys(self) -> List[str]:
        """Get all keys."""
        return self.execute_command("KEYS", "*")

    def type(self, key: str) -> str:
        """Get the type of a key."""
        return self.execute_command("TYPE", key)

    # Transaction commands
    def multi(self) -> str:
        """Start a transaction."""
        return self.execute_command("MULTI")

    def exec(self) -> List[Any]:
        """Execute a transaction."""
        return self.execute_command("EXEC")

    def discard(self) -> str:
        """Discard a transaction."""
        return self.execute_command("DISCARD")

    # Stream commands
    def xadd(self, stream: str, id: str, *field_values) -> str:
        """Add an entry to a stream."""
        args = ["XADD", stream, id]
        args.extend(field_values)
        return self.execute_command(*args)

    def xrange(self, stream: str, start: str = "-", end: str = "+") -> List:
        """Get a range of entries from a stream."""
        return self.execute_command("XRANGE", stream, start, end)

    def xread(self, streams_dict: Dict[str, str], block: Optional[int] = None) -> List:
        """Read from one or more streams.

        Args:
            streams_dict: Dictionary mapping stream names to IDs
            block: If specified, block for this many milliseconds
        """
        args = ["XREAD"]
        if block is not None:
            args.extend(["BLOCK", str(block)])

        args.append("STREAMS")
        stream_names = list(streams_dict.keys())
        stream_ids = list(streams_dict.values())

        args.extend(stream_names)
        args.extend(stream_ids)

        return self.execute_command(*args)

    # Info and config commands
    def info(self, section: str = "") -> str:
        """Get server information."""
        if section:
            return self.execute_command("INFO", section)
        return self.execute_command("INFO")

    def config_get(self, parameter: str) -> List:
        """Get server configuration parameter."""
        return self.execute_command("CONFIG", "GET", parameter)


def run_comprehensive_test(client: RedisClient) -> bool:
    """Run comprehensive test of all Redis functionality. Returns True if all tests pass."""
    print("=" * 60)
    print("COMPREHENSIVE REDIS FUNCTIONALITY TEST")
    print("=" * 60)

    tests_passed = 0
    tests_failed = 0

    def test_assert(description: str, condition: bool, expected=None, actual=None):
        nonlocal tests_passed, tests_failed
        if condition:
            print(f"✓ {description}")
            tests_passed += 1
        else:
            print(f"✗ {description}")
            if expected is not None and actual is not None:
                print(f"  Expected: {expected}")
                print(f"  Actual: {actual}")
            tests_failed += 1

    try:
        # === BASIC COMMANDS ===
        print("\n--- Basic Commands ---")

        # PING
        result = client.ping()
        test_assert("PING command", result == "PONG", "PONG", result)

        # ECHO
        result = client.echo("test message")
        test_assert("ECHO command", result == "test message", "test message", result)

        # === KEY-VALUE OPERATIONS ===
        print("\n--- Key-Value Operations ---")

        # SET and GET
        result = client.set("test:key1", "value1")
        test_assert("SET command", result == "OK", "OK", result)

        result = client.get("test:key1")
        test_assert("GET existing key", result == "value1", "value1", result)

        result = client.get("test:nonexistent")
        test_assert("GET non-existent key", result is None, None, result)

        # SET with expiration
        result = client.set("test:expire", "will expire", 1000)  # 1 second
        test_assert("SET with PX expiration", result == "OK", "OK", result)

        # INCR
        result = client.incr("test:counter")
        test_assert("INCR new key", result == 1, 1, result)

        result = client.incr("test:counter")
        test_assert("INCR existing key", result == 2, 2, result)

        # Try INCR on non-integer (should fail)
        try:
            result = client.incr("test:key1")  # This has "value1"
            test_assert("INCR on non-integer", "ERROR" in str(result), True, str(result))
        except:
            test_assert("INCR on non-integer", True, "Should error", "Got exception")

        # TYPE command
        result = client.type("test:key1")
        test_assert("TYPE for string", result == "string", "string", result)

        result = client.type("test:nonexistent")
        test_assert("TYPE for non-existent", result == "none", "none", result)

        # === KEYS COMMAND ===
        print("\n--- Keys Command ---")
        result = client.keys()
        test_assert("KEYS command returns list", isinstance(result, list), True, type(result))
        test_assert("KEYS contains test:key1", "test:key1" in result, True, result)
        test_assert("KEYS contains test:counter", "test:counter" in result, True, result)

        # === TRANSACTIONS ===
        print("\n--- Transactions ---")

        # MULTI
        result = client.multi()
        test_assert("MULTI command", result == "OK", "OK", result)

        # Commands in transaction should return QUEUED
        result = client.set("test:tx1", "tx_value1")
        test_assert("SET in transaction", result == "QUEUED", "QUEUED", result)

        result = client.set("test:tx2", "tx_value2")
        test_assert("Another SET in transaction", result == "QUEUED", "QUEUED", result)

        result = client.incr("test:counter")
        test_assert("INCR in transaction", result == "QUEUED", "QUEUED", result)

        # EXEC
        result = client.exec()
        test_assert("EXEC command returns array", isinstance(result, list), True, type(result))
        test_assert("EXEC array length", len(result) == 3, 3, len(result) if isinstance(result, list) else "not list")
        if isinstance(result, list) and len(result) >= 3:
            test_assert("First SET result", result[0] == "OK", "OK", result[0])
            test_assert("Second SET result", result[1] == "OK", "OK", result[1])
            test_assert("INCR result", result[2] == 3, 3, result[2])  # Should be 3 now

        # Verify transaction executed
        result = client.get("test:tx1")
        test_assert("Transaction SET executed", result == "tx_value1", "tx_value1", result)

        # Test DISCARD
        result = client.multi()
        test_assert("MULTI for discard test", result == "OK", "OK", result)

        result = client.set("test:discard", "should_not_exist")
        test_assert("SET before discard", result == "QUEUED", "QUEUED", result)

        result = client.discard()
        test_assert("DISCARD command", result == "OK", "OK", result)

        result = client.get("test:discard")
        test_assert("DISCARD prevented SET", result is None, None, result)

        # === STREAM OPERATIONS ===
        print("\n--- Stream Operations ---")

        # XADD
        result = client.xadd("test:stream", "*", "field1", "value1", "field2", "value2")
        test_assert("XADD command returns ID", isinstance(result, str) and "-" in result, True, f"ID format: {result}")
        first_id = result

        result = client.xadd("test:stream", "*", "field3", "value3")
        test_assert("XADD second entry", isinstance(result, str) and "-" in result, True, f"ID format: {result}")
        second_id = result

        # TYPE for stream
        result = client.type("test:stream")
        test_assert("TYPE for stream", result == "stream", "stream", result)

        # XRANGE
        result = client.xrange("test:stream", "-", "+")
        test_assert("XRANGE returns array", isinstance(result, list), True, type(result))
        test_assert("XRANGE has 2 entries", len(result) == 2, 2, len(result) if isinstance(result, list) else "not list")

        if isinstance(result, list) and len(result) >= 1:
            entry = result[0]
            test_assert("XRANGE entry format", isinstance(entry, list) and len(entry) == 2, True, f"Entry: {entry}")
            if isinstance(entry, list) and len(entry) >= 2:
                test_assert("Entry ID matches", entry[0] == first_id, first_id, entry[0])
                test_assert("Entry fields", isinstance(entry[1], list), True, type(entry[1]))

        # XRANGE with specific range
        result = client.xrange("test:stream", first_id, first_id)
        test_assert("XRANGE specific ID", isinstance(result, list) and len(result) == 1, 1, len(result) if isinstance(result, list) else "not list")

        # XREAD
        result = client.xread({"test:stream": "0-0"})
        test_assert("XREAD returns array", isinstance(result, list), True, type(result))
        test_assert("XREAD has stream entry", len(result) == 1, 1, len(result) if isinstance(result, list) else "not list")

        if isinstance(result, list) and len(result) >= 1:
            stream_result = result[0]
            test_assert("XREAD stream format", isinstance(stream_result, list) and len(stream_result) == 2, True, f"Stream result: {stream_result}")
            if isinstance(stream_result, list) and len(stream_result) >= 2:
                test_assert("XREAD stream name", stream_result[0] == "test:stream", "test:stream", stream_result[0])

        # === INFO AND CONFIG ===
        print("\n--- Info and Config ---")

        # INFO
        result = client.info("REPLICATION")
        test_assert("INFO REPLICATION", isinstance(result, str) and "role:" in result, True, f"Contains role: {'role:' in str(result)}")

        # CONFIG GET
        result = client.config_get("dir")
        test_assert("CONFIG GET dir returns array", isinstance(result, list), True, type(result))
        if isinstance(result, list):
            test_assert("CONFIG GET dir format", len(result) == 2, 2, len(result))
            if len(result) >= 1:
                test_assert("CONFIG GET dir key", result[0] == "dir", "dir", result[0])

        result = client.config_get("dbfilename")
        test_assert("CONFIG GET dbfilename", isinstance(result, list), True, type(result))

        # === CLEANUP ===
        print("\n--- Cleanup Test Keys ---")

        # Clean up test keys (optional, but good practice)
        cleanup_keys = ["test:key1", "test:counter", "test:tx1", "test:tx2", "test:expire"]
        # Note: Can't delete streams or other keys easily with current client, but that's okay

    except Exception as e:
        print(f"\n✗ Test suite failed with exception: {e}")
        import traceback
        traceback.print_exc()
        tests_failed += 1

    # === SUMMARY ===
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Tests Passed: {tests_passed}")
    print(f"Tests Failed: {tests_failed}")
    print(f"Total Tests:  {tests_passed + tests_failed}")

    if tests_failed == 0:
        print("🎉 ALL TESTS PASSED! Redis functionality is working correctly.")
        return True
    else:
        print(f"❌ {tests_failed} TESTS FAILED. Please check the issues above.")
        return False


def interactive_mode(client: RedisClient) -> None:
    """Run an interactive Redis client session."""
    print(f"Connected to Redis server at {client.host}:{client.port}")
    print("Type 'exit' to quit, 'help' for command list")

    while True:
        try:
            command = input("redis> ").strip()

            if not command:
                continue

            if command.lower() == "exit":
                break

            if command.lower() == "help":
                print("""
Available commands:
  PING - Test connection
  ECHO <message> - Echo a message
  SET <key> <value> [PX <milliseconds>] - Set key to value with optional expiration
  GET <key> - Get value for key
  INCR <key> - Increment value of key
  KEYS - List all keys
  TYPE <key> - Get type of key
  MULTI - Start a transaction
  EXEC - Execute a transaction
  DISCARD - Discard a transaction
  XADD <stream> <id> <field> <value> [<field> <value>...] - Add to stream
  XRANGE <stream> <start> <end> - Get range from stream
  XREAD [BLOCK <milliseconds>] STREAMS <stream> <id> [<stream> <id>...] - Read from streams
  INFO [section] - Get server information
  CONFIG GET <parameter> - Get configuration parameter
                """)
                continue

            # Parse the command
            args = []
            current_arg = ""
            in_quotes = False

            for char in command:
                if char == '"' and (not current_arg or current_arg[-1] != '\\'):
                    in_quotes = not in_quotes
                elif char.isspace() and not in_quotes:
                    if current_arg:
                        args.append(current_arg)
                        current_arg = ""
                else:
                    current_arg += char

            if current_arg:
                args.append(current_arg)

            if not args:
                continue

            # Special case handling for common commands
            cmd = args[0].upper()
            if cmd == "SET" and len(args) >= 3:
                if len(args) >= 5 and args[3].upper() == "PX":
                    result = client.set(args[1], args[2], int(args[4]))
                else:
                    result = client.set(args[1], args[2])

            elif cmd == "GET" and len(args) >= 2:
                result = client.get(args[1])

            elif cmd == "INCR" and len(args) >= 2:
                result = client.incr(args[1])

            elif cmd == "PING":
                result = client.ping()

            elif cmd == "ECHO" and len(args) >= 2:
                result = client.echo(args[1])

            elif cmd == "KEYS":
                result = client.keys()

            elif cmd == "MULTI":
                result = client.multi()

            elif cmd == "EXEC":
                result = client.exec()

            elif cmd == "DISCARD":
                result = client.discard()

            else:
                # Generic command execution
                result = client.execute_command(*args)

            print(result)

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")

    client.disconnect()
    print("Disconnected from Redis server")


def run_basic_demo(client: RedisClient) -> None:
    """Run a basic demo of Redis commands."""
    print("Running basic Redis commands demo:")

    # Basic commands
    print("\n--- Basic Commands ---")
    print("PING:", client.ping())
    print("ECHO 'Hello Redis!':", client.echo("Hello Redis!"))

    # Key-value operations
    print("\n--- Key-Value Operations ---")
    print("SET user:1 'John':", client.set("user:1", "John"))
    print("GET user:1:", client.get("user:1"))
    print("INCR counter:", client.incr("counter"))
    print("INCR counter:", client.incr("counter"))
    print("GET counter:", client.get("counter"))

    # Expiration
    print("\n--- Expiration ---")
    print("SET temp 'will expire' PX 5000:", client.set("temp", "will expire", 5000))
    print("GET temp (before expiration):", client.get("temp"))

    # Transaction
    print("\n--- Transaction ---")
    print("MULTI:", client.multi())
    print("SET tx:1 'value1':", client.set("tx:1", "value1"))
    print("SET tx:2 'value2':", client.set("tx:2", "value2"))
    print("INCR counter:", client.incr("counter"))
    print("EXEC:", client.exec())

    print("\n--- Keys ---")
    print("KEYS:", client.keys())

    # Stream operations
    print("\n--- Stream Operations ---")
    print("XADD mystream * field1 value1 field2 value2:",
          client.xadd("mystream", "*", "field1", "value1", "field2", "value2"))
    print("XRANGE mystream - +:", client.xrange("mystream"))

    # Non-blocking XREAD
    print("\nXREAD STREAMS mystream 0-0:", client.xread({"mystream": "0-0"}))


def main():
    parser = argparse.ArgumentParser(description="Redis Client")
    parser.add_argument("--host", default="localhost", help="Redis server host")
    parser.add_argument("--port", type=int, default=6379, help="Redis server port")
    parser.add_argument("--demo", action="store_true", help="Run demo commands")
    parser.add_argument("--test", action="store_true", help="Run comprehensive functionality test")

    args = parser.parse_args()

    client = RedisClient(args.host, args.port)

    if not client.connect():
        print(f"Failed to connect to Redis server at {args.host}:{args.port}")
        return

    if args.test:
        success = run_comprehensive_test(client)
        client.disconnect()
        exit(0 if success else 1)
    elif args.demo:
        run_basic_demo(client)
        client.disconnect()
    else:
        interactive_mode(client)


if __name__ == "__main__":
    main()
