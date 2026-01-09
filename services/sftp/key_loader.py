"""Key loading and PPK conversion functionality."""
import os
import io
import subprocess
import paramiko

# Try to import puttykeys for .ppk file support
try:
    from puttykeys import ppkraw_to_openssh
    PPK_SUPPORT = True
except ImportError:
    PPK_SUPPORT = False
    print("[WARN] puttykeys library not installed. .ppk files will not be supported. Install with: pip install puttykeys")

# Try to import cryptography for alternative PPK support
try:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    CRYPTOGRAPHY_SUPPORT = True
except ImportError:
    CRYPTOGRAPHY_SUPPORT = False


def get_ppk_public_key(key_path: str, password: str = None):
    """
    Extract the public key from a PPK file for verification.
    This helps ensure the converted key matches what's on the server.
    """
    try:
        # Try to extract public key using puttygen
        cmd = ['puttygen', key_path, '-O', 'public-openssh']
        if password:
            process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate(input=password, timeout=5)
            if process.returncode == 0:
                return stdout.strip()
        else:
            result = subprocess.run(cmd, capture_output=True, timeout=5, text=True)
            if result.returncode == 0:
                return result.stdout.strip()
    except Exception as e:
        print(f"[DEBUG] Could not extract public key from PPK: {e}")
    return None


def load_private_key(key_path: str, password: str = None):
    """
    Load a private key from either .ppk or .pem format.
    
    :param key_path: Path to the private key file (.ppk or .pem)
    :param password: Optional passphrase for encrypted keys
    :return: A Paramiko PKey object
    """
    if not os.path.exists(key_path):
        raise FileNotFoundError(f"Key file not found: {key_path}")
    
    file_extension = os.path.splitext(key_path)[1].lower()
    
    # Handle .ppk files (PuTTY private key format)
    if file_extension == '.ppk':
        # First, try to use puttygen directly (more reliable than puttykeys library)
        try:
            print("[INFO] Converting .ppk file using puttygen...")
            converted_key_dir = os.path.join(os.path.dirname(key_path), 'converted_keys')
            os.makedirs(converted_key_dir, exist_ok=True)
            base_name = os.path.splitext(os.path.basename(key_path))[0]
            tmp_key_path = os.path.join(converted_key_dir, f"{base_name}_from_ppk.pem")
            
            # Convert using puttygen - try traditional format first
            cmd = ['puttygen', key_path, '-O', 'private-openssh', '-o', tmp_key_path]
            result = subprocess.run(cmd, capture_output=True, timeout=10, text=True)
            
            # If that failed, try newer format
            if result.returncode != 0:
                print(f"[DEBUG] Traditional format failed, trying OpenSSH format...")
                cmd_new = ['puttygen', key_path, '-O', 'private-openssh-new', '-o', tmp_key_path]
                result = subprocess.run(cmd_new, capture_output=True, timeout=10, text=True)
            
            if result.returncode == 0 and os.path.exists(tmp_key_path) and os.path.getsize(tmp_key_path) > 0:
                # Verify public keys match
                try:
                    ppk_pub = get_ppk_public_key(key_path, password)
                    pem_pub_result = subprocess.run(
                        ['ssh-keygen', '-y', '-f', tmp_key_path],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if pem_pub_result.returncode == 0:
                        pem_pub = pem_pub_result.stdout.strip()
                        # Compare just the key data (ignore key type prefix)
                        ppk_key_data = ppk_pub.split()[1] if len(ppk_pub.split()) > 1 else ppk_pub
                        pem_key_data = pem_pub.split()[1] if len(pem_pub.split()) > 1 else pem_pub
                        if ppk_key_data == pem_key_data:
                            print("[INFO] Public keys match - conversion successful")
                        else:
                            print("[WARN] Public keys differ - but continuing anyway")
                except Exception as pub_err:
                    print(f"[DEBUG] Could not verify public keys: {pub_err}")
                
                # Set correct permissions
                os.chmod(tmp_key_path, 0o600)
                
                # Load the converted key
                for key_class in [paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key]:
                    try:
                        key = key_class.from_private_key_file(tmp_key_path, password=password)
                        print(f"[INFO] Successfully loaded converted .ppk key as {key_class.__name__}")
                        print(f"[INFO] Converted key saved at: {tmp_key_path}")
                        print(f"[INFO] You can use this file directly by setting SFTP_KEY_PATH={tmp_key_path}")
                        return key
                    except Exception as key_err:
                        continue
                
                raise ValueError(f"Converted key file exists but could not be loaded by paramiko")
            else:
                error_msg = result.stderr if result.stderr else "Unknown error"
                print(f"[DEBUG] puttygen conversion failed: {error_msg}")
                raise ValueError(f"puttygen failed to convert .ppk file: {error_msg}")
        except (FileNotFoundError, subprocess.TimeoutExpired) as e:
            print(f"[WARN] puttygen not available or timed out: {e}")
            # Fall through to puttykeys library method
        except Exception as e:
            print(f"[WARN] puttygen conversion failed: {e}")
            # Fall through to puttykeys library method
        
        # Fallback to puttykeys library if puttygen failed
        if not PPK_SUPPORT:
            raise ImportError(
                "Neither puttygen command-line tool nor puttykeys library is available for .ppk files. "
                "Please install one of:\n"
                "  - puttygen: sudo apt-get install putty-tools (Linux) or install PuTTY (Windows)\n"
                "  - puttykeys: pip install puttykeys"
            )
        
        # Extract and display public key for debugging
        try:
            public_key = get_ppk_public_key(key_path, password)
            if public_key:
                print(f"[DEBUG] Public key from PPK file: {public_key[:50]}...")
        except Exception:
            pass  # Non-critical, continue
        
        try:
            # Read the .ppk file - try both text and binary modes
            ppk_data = None
            try:
                with open(key_path, 'r', encoding='utf-8') as f:
                    ppk_data = f.read()
            except (UnicodeDecodeError, ValueError):
                # If text mode fails, try binary mode
                with open(key_path, 'rb') as f:
                    ppk_data = f.read().decode('utf-8', errors='ignore')
            
            if not ppk_data:
                raise ValueError("Failed to read .ppk file")
            
            # Convert .ppk to OpenSSH format using puttykeys
            # Try different parameter combinations
            passphrase_str = password.encode('utf-8') if password else None
            openssh_key = None
            conversion_error = None
            
            # Method 1: Try with passphrase as string
            try:
                print(f"[DEBUG] Attempting PPK conversion with puttykeys (method 1: string passphrase)")
                openssh_key = ppkraw_to_openssh(ppk_data, passphrase=password if password else '')
                if openssh_key:
                    print(f"[DEBUG] PPK conversion successful (method 1)")
            except (TypeError, ValueError) as e1:
                conversion_error = e1
                print(f"[DEBUG] Method 1 failed: {e1}")
                # Method 2: Try with passphrase as bytes
                try:
                    print(f"[DEBUG] Attempting PPK conversion (method 2: bytes passphrase)")
                    openssh_key = ppkraw_to_openssh(ppk_data, passphrase=passphrase_str)
                    if openssh_key:
                        print(f"[DEBUG] PPK conversion successful (method 2)")
                except (TypeError, ValueError) as e2:
                    conversion_error = e2
                    print(f"[DEBUG] Method 2 failed: {e2}")
                    # Method 3: Try without passphrase parameter
                    try:
                        print(f"[DEBUG] Attempting PPK conversion (method 3: no passphrase param)")
                        openssh_key = ppkraw_to_openssh(ppk_data)
                        if openssh_key:
                            print(f"[DEBUG] PPK conversion successful (method 3)")
                    except Exception as e3:
                        conversion_error = e3
                        print(f"[DEBUG] Method 3 failed: {e3}")
                        raise ValueError(
                            f"Failed to convert .ppk file. Tried multiple methods. "
                            f"Last error: {e3}. Previous errors: {e1}, {e2}"
                        )
            
            if not openssh_key or not openssh_key.strip():
                # Try fallback: use puttygen command-line tool if available
                try:
                    print("[INFO] puttykeys conversion failed, trying puttygen as fallback...")
                    # Save to a persistent location for debugging
                    converted_key_dir = os.path.join(os.path.dirname(key_path), 'converted_keys')
                    os.makedirs(converted_key_dir, exist_ok=True)
                    base_name = os.path.splitext(os.path.basename(key_path))[0]
                    tmp_key_path = os.path.join(converted_key_dir, f"{base_name}_converted.pem")
                    
                    # Try to convert using puttygen
                    cmd = ['puttygen', key_path, '-O', 'private-openssh', '-o', tmp_key_path]
                    if password:
                        process = subprocess.Popen(
                            cmd,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True
                        )
                        stdout, stderr = process.communicate(input=password, timeout=10)
                        result = type('obj', (object,), {'returncode': process.returncode})()
                    else:
                        result = subprocess.run(cmd, capture_output=True, timeout=10, text=True)
                    
                    # If that failed, try the newer format
                    if result.returncode != 0:
                        print(f"[DEBUG] puttygen conversion failed, trying newer format...")
                        cmd_new = ['puttygen', key_path, '-O', 'private-openssh-new', '-o', tmp_key_path]
                        if password:
                            process = subprocess.Popen(
                                cmd_new,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                text=True
                            )
                            stdout, stderr = process.communicate(input=password, timeout=10)
                            result = type('obj', (object,), {'returncode': process.returncode})()
                        else:
                            result = subprocess.run(cmd_new, capture_output=True, timeout=10, text=True)
                    
                    # Debug output
                    if result.returncode != 0:
                        error_msg = stderr if password else result.stderr
                        print(f"[DEBUG] puttygen error: {error_msg}")
                        print(f"[DEBUG] puttygen stdout: {stdout if password else result.stdout}")
                    
                    if result.returncode == 0 and os.path.exists(tmp_key_path):
                        # Verify the converted key file exists and has content
                        if os.path.getsize(tmp_key_path) == 0:
                            print("[WARN] puttygen created empty key file")
                            if os.path.exists(tmp_key_path):
                                os.unlink(tmp_key_path)
                        else:
                            print(f"[INFO] Converted key saved to: {tmp_key_path}")
                            print(f"[INFO] You can inspect this file and use it directly by updating SFTP_KEY_PATH in .env")
                            
                            # Extract and compare public keys
                            try:
                                # Get public key from converted file
                                cmd_pub = ['ssh-keygen', '-y', '-f', tmp_key_path]
                                if password:
                                    process_pub = subprocess.Popen(
                                        cmd_pub,
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        text=True
                                    )
                                    stdout_pub, stderr_pub = process_pub.communicate(input=password, timeout=5)
                                    if process_pub.returncode == 0:
                                        converted_pub_key = stdout_pub.strip()
                                        print(f"[DEBUG] Public key from converted file: {converted_pub_key[:60]}...")
                                else:
                                    result_pub = subprocess.run(cmd_pub, capture_output=True, timeout=5, text=True)
                                    if result_pub.returncode == 0:
                                        converted_pub_key = result_pub.stdout.strip()
                                        print(f"[DEBUG] Public key from converted file: {converted_pub_key[:60]}...")
                                
                                # Get public key from original PPK
                                ppk_pub_key = get_ppk_public_key(key_path, password)
                                if ppk_pub_key:
                                    print(f"[DEBUG] Public key from PPK file: {ppk_pub_key[:60]}...")
                                    if 'converted_pub_key' in locals() and converted_pub_key == ppk_pub_key:
                                        print("[INFO] Public keys match - conversion preserved key correctly")
                                    else:
                                        print("[WARN] Public keys differ - this might be the issue!")
                            except Exception as pub_err:
                                print(f"[DEBUG] Could not compare public keys: {pub_err}")
                            
                            # Load the converted key - try all key types
                            key_loaded = False
                            for key_class in [paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key]:
                                try:
                                    key = key_class.from_private_key_file(tmp_key_path, password=password)
                                    # Verify we got a valid key object
                                    if key:
                                        # Try to get the key fingerprint for debugging
                                        try:
                                            fingerprint = key.get_fingerprint().hex() if hasattr(key, 'get_fingerprint') else 'N/A'
                                            print(f"[DEBUG] Loaded key type: {key_class.__name__}, fingerprint: {fingerprint}")
                                        except:
                                            pass
                                        # Don't delete - keep for inspection and potential reuse
                                        print("[INFO] Successfully converted .ppk using puttygen")
                                        print(f"[INFO] Converted key file kept at: {tmp_key_path}")
                                        print(f"[INFO] Consider updating SFTP_KEY_PATH to use this file directly for better performance")
                                        return key
                                except Exception as key_err:
                                    print(f"[DEBUG] Failed to load as {key_class.__name__}: {key_err}")
                                    continue
                            
                            if not key_loaded:
                                print("[WARN] Could not load converted key with any key type")
                                print(f"[INFO] Converted key file kept at {tmp_key_path} for inspection")
                except (FileNotFoundError, subprocess.TimeoutExpired, Exception) as fallback_error:
                    pass  # Fallback failed, continue with original error
                
                raise ValueError(
                    "Unable to convert .ppk file to OpenSSH format. "
                    "The conversion returned empty or None. "
                    "This might indicate:\n"
                    "  1. Unsupported PPK format (try converting with puttygen manually)\n"
                    "  2. Incorrect passphrase\n"
                    "  3. Corrupted .ppk file\n"
                    "  4. Issue with puttykeys library\n\n"
                    "Suggested solutions:\n"
                    "  - Convert the .ppk file to OpenSSH format manually using:\n"
                    "    puttygen your_key.ppk -O private-openssh -o your_key.pem\n"
                    "  - Or verify the .ppk file and passphrase are correct"
                )
            
            # Save the converted key to a file and use the file path directly
            # This is more reliable than using StringIO
            converted_key_dir = os.path.join(os.path.dirname(key_path), 'converted_keys')
            os.makedirs(converted_key_dir, exist_ok=True)
            base_name = os.path.splitext(os.path.basename(key_path))[0]
            converted_key_path = os.path.join(converted_key_dir, f"{base_name}_converted.pem")
            
            try:
                # Write the converted key to file
                with open(converted_key_path, 'w') as f:
                    f.write(openssh_key)
                print(f"[INFO] Converted key saved to: {converted_key_path}")
                print(f"[INFO] You can use this file directly by updating SFTP_KEY_PATH in .env")
                
                # Try loading from file - this is more reliable
                last_error = None
                for key_class in [paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key]:
                    try:
                        return key_class.from_private_key_file(converted_key_path, password=password)
                    except paramiko.ssh_exception.SSHException as e:
                        last_error = e
                        continue
                
                # If all key types failed, try loading without password
                if password:
                    for key_class in [paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key]:
                        try:
                            return key_class.from_private_key_file(converted_key_path, password=None)
                        except paramiko.ssh_exception.SSHException:
                            continue
                
                raise ValueError(
                    f"Failed to load converted OpenSSH key from file. "
                    f"Tried all key types (RSA, DSS, ECDSA, Ed25519). "
                    f"Last error: {last_error}. "
                    f"Converted key file saved at: {converted_key_path}"
                )
            except Exception as file_err:
                # Fallback to StringIO if file write fails
                print(f"[WARN] Could not save converted key to file: {file_err}, using in-memory key")
                key_file = io.StringIO(openssh_key)
                # Try different key types
                last_error = None
                for key_class in [paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key]:
                    try:
                        key_file.seek(0)
                        return key_class.from_private_key(key_file, password=password)
                    except paramiko.ssh_exception.SSHException as e:
                        last_error = e
                        continue
                
                # If all key types failed, try loading without password
                if password:
                    for key_class in [paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key]:
                        try:
                            key_file.seek(0)
                            return key_class.from_private_key(key_file, password=None)
                        except paramiko.ssh_exception.SSHException:
                            continue
                
                raise ValueError(
                    f"Failed to load converted OpenSSH key into paramiko. "
                    f"Tried all key types (RSA, DSS, ECDSA, Ed25519). "
                    f"Last error: {last_error}"
                )
            
        except ValueError as ve:
            # Re-raise ValueError with more context
            raise ValueError(f"Failed to load .ppk key file: {ve}")
        except Exception as e:
            raise ValueError(
                f"Failed to load .ppk key file: {type(e).__name__}: {e}. "
                f"Please verify the .ppk file format and passphrase (if required)."
            )
    
    # Handle .pem and other standard formats
    else:
        # First, try loading with paramiko's general key loader (supports OpenSSH format)
        try:
            # This handles both traditional PEM and OpenSSH private key formats
            key = paramiko.RSAKey.from_private_key_file(key_path, password=password)
            print(f"[DEBUG] Successfully loaded key as RSA (traditional PEM format)")
            return key
        except Exception as e1:
            print(f"[DEBUG] Failed to load as RSA (traditional PEM): {e1}")
            
            # Try OpenSSH format - paramiko should handle this automatically, but let's be explicit
            try:
                # Read the file to check format
                with open(key_path, 'r') as f:
                    key_content = f.read()
                
                # If it's OpenSSH format, try loading it directly
                if 'BEGIN OPENSSH PRIVATE KEY' in key_content:
                    print(f"[DEBUG] Detected OpenSSH format, trying direct load...")
                    # Paramiko should handle OpenSSH format automatically
                    key_file_obj = io.StringIO(key_content)
                    key = paramiko.RSAKey.from_private_key(key_file_obj, password=password)
                    print(f"[DEBUG] Successfully loaded OpenSSH format key")
                    return key
            except Exception as e2:
                print(f"[DEBUG] Failed to load OpenSSH format: {e2}")
        
        # Fallback: Try different key types that paramiko supports (skip DSS/DSA to avoid errors)
        key_types = [
            (paramiko.RSAKey, "RSA"),
            (paramiko.ECDSAKey, "ECDSA"),
            (paramiko.Ed25519Key, "Ed25519"),
            # Skip DSSKey as it causes "q must be exactly 160, 224, or 256 bits long" errors
        ]
        
        for key_class, key_name in key_types:
            try:
                key = key_class.from_private_key_file(key_path, password=password)
                print(f"[DEBUG] Successfully loaded key as {key_name}")
                return key
            except (paramiko.ssh_exception.SSHException, ValueError) as e:
                print(f"[DEBUG] Failed to load as {key_name}: {e}")
                continue
        
        # If all attempts failed, raise an error
        raise ValueError(
            f"Unable to load private key from {key_path}. "
            f"Supported formats: .ppk, .pem (RSA, DSS, ECDSA, Ed25519, OpenSSH). "
            f"Please verify the key file is valid and has correct permissions (600)."
        )

