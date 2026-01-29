# Installing dbt

## Current Issue
You're getting `command not found: dbt` because dbt is not installed in your environment.

## Installation Options

### Option 1: Install with pip (Recommended)

Since you're using conda (base environment), you can install dbt with pip:

```bash
pip install dbt-core dbt-postgres
```

Or install from your requirements.txt:
```bash
pip install -r requirements.txt
```

**Note:** If you get network errors, check your internet connection and try again.

### Option 2: Install with conda

```bash
conda install -c conda-forge dbt-core dbt-postgres
```

### Option 3: Install in a virtual environment (Best Practice)

```bash
# Create a virtual environment
python -m venv venv

# Activate it
source venv/bin/activate  # On Mac/Linux
# OR
venv\Scripts\activate  # On Windows

# Install dbt
pip install dbt-core dbt-postgres
```

## Verify Installation

After installation, verify dbt is working:

```bash
dbt --version
```

You should see something like:
```
Core:
  - installed: 1.7.x
  - latest:    1.7.x - Up to date!

Plugins:
  - postgres: 1.7.x - Up to date!
```

## Test Your Connection

Once dbt is installed, test your connection:

```bash
dbt debug
```

This will:
- Check your profiles.yml configuration
- Test the database connection
- Verify project setup

## Troubleshooting

### If `dbt` command still not found after installation:

1. **Check if it's in your PATH:**
   ```bash
   which dbt
   ```

2. **Try using python module syntax:**
   ```bash
   python -m dbt --version
   ```

3. **Check installation location:**
   ```bash
   pip show dbt-core
   ```

4. **Restart your terminal** after installation

### If you get permission errors:

Use `--user` flag:
```bash
pip install --user dbt-core dbt-postgres
```

### If network issues persist:

1. Check your internet connection
2. Try using a different network
3. Check if you're behind a corporate firewall/proxy
4. Try installing from a requirements file that's already downloaded

## Next Steps

Once dbt is installed:
1. Run `dbt debug` to verify connection
2. Run `dbt list` to see available models
3. Start writing your SQL models!
