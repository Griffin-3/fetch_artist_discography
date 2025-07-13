import paramiko
import time
import sys
import requests
import sqlite3
from datetime import datetime, timedelta

class ChangeFiberIP:
    def __init__(self, sqlite3_file, table, days=30):
        """Initialize the IP change module with database and router settings."""
        # Database configuration
        self.sqlite3_file = sqlite3_file
        self.table = table
        self.days = days

        # Router configuration
        self.router_ip = os.getenv("router_ip")
        self.username = os.getenv("router_username")
        self.password = os.getenv("router_password")
        self.interface = os.getenv("router_interface")
        self.ip_check_url = os.getenv("ip_check_url")

        # Retry configuration
        self.max_attempts_per_cycle = 5
        self.max_cycles = 5
        self.retry_wait_seconds = 3600  # 1 hour

        # Initialize database
        self._init_db()

    def _init_db(self):
        """Initialize the SQLite database and create the table if it doesn't exist."""
        try:
            conn = sqlite3.connect(self.sqlite3_file)
            cursor = conn.cursor()
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    ip TEXT PRIMARY KEY,
                    date INTEGER,
                    status INTEGER
                )
            """)
            conn.commit()
            conn.close()
            # ~ print(f"Database {self.sqlite3_file} initialized with table {self.table}.")
        except sqlite3.Error as e:
            print(f"Failed to initialize database: {e}")
            sys.exit(1)

    def _check_ip_in_db(self, ip):
        """Check if the IP exists in the database and return its status and date."""
        try:
            conn = sqlite3.connect(self.sqlite3_file)
            cursor = conn.cursor()
            cursor.execute(f"SELECT date, status FROM {self.table} WHERE ip = ?", (ip,))
            result = cursor.fetchone()
            conn.close()
            return result  # Returns (date, status) or None if not found
        except sqlite3.Error as e:
            print(f"Failed to check IP in database: {e}")
            return None

    def _update_ip_date(self, ip, today_date):
        """Update the date of an existing IP in the database."""
        try:
            conn = sqlite3.connect(self.sqlite3_file)
            cursor = conn.cursor()
            cursor.execute(f"UPDATE {self.table} SET date = ? WHERE ip = ?", (today_date, ip))
            conn.commit()
            conn.close()
            print(f"Updated date for IP {ip} to {today_date}")
        except sqlite3.Error as e:
            print(f"Failed to update IP date: {e}")

    def _insert_new_ip(self, ip, today_date):
        """Insert a new IP into the database with today's date and status = 0."""
        try:
            conn = sqlite3.connect(self.sqlite3_file)
            cursor = conn.cursor()
            cursor.execute(f"INSERT INTO {self.table} (ip, date, status) VALUES (?, ?, ?)", (ip, today_date, 0))
            conn.commit()
            conn.close()
            print(f"Inserted new IP {ip} with date {today_date} and status 0")
        except sqlite3.Error as e:
            print(f"Failed to insert new IP: {e}")

    def _is_ip_invalid(self, ip_info, today_date):
        """Check if the IP is banned (status = 1) or too recent (less than self.days old)."""
        if not ip_info:
            return False  # IP not in database, not invalid
        ip_date, status = ip_info
        if status == 1:
            print(f"IP is banned (status = 1)")
            return True
        thirty_days_ago = int((datetime.strptime(str(today_date), "%Y%m%d") - timedelta(days=self.days)).strftime("%Y%m%d"))
        if ip_date > thirty_days_ago:
            print(f"IP is too recent (date {ip_date}, less than {self.days} days ago)")
            return True
        return False

    def _get_public_ip(self):
        """Fetch the current public IP address from the specified URL."""
        try:
            response = requests.get(self.ip_check_url, timeout=10)
            response.raise_for_status()
            return response.text.strip()
        except requests.RequestException as e:
            print(f"Failed to fetch public IP: {e}")
            return None

    def _ssh_connect(self):
        """Establish an SSH connection to the router."""
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(self.router_ip, username=self.username, password=self.password, timeout=10)
            return client
        except Exception as e:
            print(f"Failed to connect: {e}")
            return None

    def _execute_command(self, client, command):
        """Execute a command via SSH and return the output."""
        try:
            stdin, stdout, stderr = client.exec_command(command)
            output = stdout.read().decode().strip()
            error = stderr.read().decode().strip()
            if error:
                print(f"Command error: {error}")
            return output
        except Exception as e:
            print(f"Command execution failed: {e}")
            return None

    def _change_mac_address(self, client):
        """Change the MAC address of the specified network interface using UCI."""
        try:
            # Get current MAC address
            fetch_cmd = f"uci get network.{self.interface}.macaddr"
            mac_addr = self._execute_command(client, fetch_cmd)
            bytes = mac_addr.split(":")
            val = int(bytes[3], 16) + 1
            bytes[3] = f"{val if val<=254 else 1:02X}"
            new_mac = ":".join(bytes)
            print(f"Incrementing MAC address from {mac_addr} to {new_mac}...")

            # Set the new MAC address
            set_mac_cmd = f"uci set network.{self.interface}.macaddr='{new_mac}'"
            self._execute_command(client, set_mac_cmd)

            # Commit the changes
            commit_cmd = "uci commit network"
            self._execute_command(client, commit_cmd)

            # Restart the network to apply changes
            restart_cmd = "/etc/init.d/network restart"
            self._execute_command(client, restart_cmd)

            # Verify the change
            fetch_cmd = f"uci get network.{self.interface}.macaddr"
            mac_addr = self._execute_command(client, fetch_cmd)
            if mac_addr == new_mac:
                print(f"Successfully changed MAC address of {self.interface} to {new_mac}")
                return True
            else:
                print(f"Failed to verify MAC address change. Current MAC: {mac_addr}")
                return False
        except Exception as e:
            print(f"Failed to change MAC address: {e}")
            return False

    def get_current_ip_age(self):
        """Return the age (in days) of the current public IP, or None if not found."""
        try:
            current_ip = self._get_public_ip()
            if not current_ip:
                print("Could not retrieve current public IP.")
                return None

            ip_info = self._check_ip_in_db(current_ip)
            if not ip_info:
                print(f"IP {current_ip} not found in database.")
                return None

            ip_date, _ = ip_info
            today = datetime.strptime(datetime.now().strftime("%Y%m%d"), "%Y%m%d")
            ip_date_dt = datetime.strptime(str(ip_date), "%Y%m%d")
            age_days = (today - ip_date_dt).days
            print(f"IP {current_ip} is {age_days} days old.")
            return age_days
        except Exception as e:
            print(f"Failed to get IP age: {e}")
            return None

    def change_ip(self):
        """Change the router's MAC address to get a new IP, ensuring it's not banned or too recent."""
        today_date = int(datetime.now().strftime("%Y%m%d"))
        print(f"Today's date: {today_date}")

        # Get initial public IP
        initial_ip = self._get_public_ip()
        if initial_ip:
            print(f"Initial public IP address: {initial_ip}")
        else:
            print("Could not retrieve initial public IP address.")

        # Connect to the router
        ssh_client = self._ssh_connect()
        if not ssh_client:
            print("Exiting due to connection failure.")
            return False

        try:
            cycle_count = 0
            while cycle_count < self.max_cycles:
                attempt_count = 0
                while attempt_count < self.max_attempts_per_cycle:
                    attempt_count += 1
                    print(f"Attempt {attempt_count} of {self.max_attempts_per_cycle} in cycle {cycle_count + 1}")

                    # Change MAC address
                    if not self._change_mac_address(ssh_client):
                        print("MAC address change failed.")
                        continue

                    # Wait for network to stabilize
                    print("Waiting for network to stabilize...")
                    time.sleep(10)

                    # Get new public IP
                    new_ip = self._get_public_ip()
                    if not new_ip:
                        print("ERROR: Could not retrieve new public IP address.")
                        continue

                    print(f"New public IP address: {new_ip}")
                    if new_ip != initial_ip:
                        print("SUCCESS: Public IP address has changed successfully.")
                    else:
                        print("FAIL: Public IP address remains the same.")

                    # Check IP in database
                    ip_info = self._check_ip_in_db(new_ip)
                    if ip_info:
                        # IP exists, update date
                        self._update_ip_date(new_ip, today_date)
                        if self._is_ip_invalid(ip_info, today_date):
                            print(f"IP {new_ip} is invalid. Trying again...")
                            initial_ip = new_ip  # Update to avoid false success
                            continue
                    else:
                        # New IP, insert into database
                        self._insert_new_ip(new_ip, today_date)

                    # Valid IP found, return success
                    print(f"Valid IP {new_ip} obtained.")
                    return True

                # All attempts in this cycle failed
                cycle_count += 1
                if cycle_count < self.max_cycles:
                    print(f"All attempts in cycle {cycle_count} failed. Waiting {self.retry_wait_seconds} seconds before next cycle...")
                    time.sleep(self.retry_wait_seconds)
                else:
                    print("Max cycles reached. UNABLE to get usable IP.")
                    sys.exit("UNABLE to get usable IP.")

        finally:
            ssh_client.close()
            print("SSH connection closed.")

        return False

    def set_banned_ip(self):
        """Mark the current public IP as banned (status = 1) in the database."""
        try:
            current_ip = self._get_public_ip()
            if not current_ip:
                print("Could not retrieve current public IP.")
                return False

            today_date = int(datetime.now().strftime("%Y%m%d"))
            ip_info = self._check_ip_in_db(current_ip)
            if ip_info:
                # Update existing IP to banned
                try:
                    conn = sqlite3.connect(self.sqlite3_file)
                    cursor = conn.cursor()
                    cursor.execute(f"UPDATE {self.table} SET status = 1, date = ? WHERE ip = ?", (today_date, current_ip))
                    conn.commit()
                    conn.close()
                    print(f"IP {current_ip} marked as banned (status = 1) with date {today_date}.")
                    return True
                except sqlite3.Error as e:
                    print(f"Failed to update IP status: {e}")
                    return False
            else:
                # Insert new IP as banned
                try:
                    conn = sqlite3.connect(self.sqlite3_file)
                    cursor = conn.cursor()
                    cursor.execute(f"INSERT INTO {self.table} (ip, date, status) VALUES (?, ?, ?)", (current_ip, today_date, 1))
                    conn.commit()
                    conn.close()
                    print(f"IP {current_ip} inserted as banned (status = 1) with date {today_date}.")
                    return True
                except sqlite3.Error as e:
                    print(f"Failed to insert banned IP: {e}")
                    return False
        except Exception as e:
            print(f"Failed to set banned IP: {e}")
            return False
