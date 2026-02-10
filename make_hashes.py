import bcrypt

passwords = ["Admin@123", "Manager@123"]  # change these

for p in passwords:
    h = bcrypt.hashpw(p.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    print(f"{p} => {h}")
