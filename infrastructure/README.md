# Infrastructure Setup

## Snowflake

Run `snowflake_setup.sql` as ACCOUNTADMIN to set up:
- Warehouse: ANALYTICS_WH (X-Small, auto-suspends 60s)
- Databases: BRONZE_DB, SILVER_DB, GOLD_DB
- Schemas per medallion layer
- Roles: PIPELINE_ROLE, REPORTING_ROLE
- Users: PIPELINE_USER, REPORTING_USER

## AWS

S3 buckets created via AWS CLI:
- retail-intel-bronze (versioning enabled)
- retail-intel-silver
- retail-intel-gold
- retail-intel-archive

## Design Decisions

See `docs/design_decisions.md` for architectural choices and known limitations.

## EC2 — Clickstream Generator

### Instance Details
- AMI: Amazon Linux 2023
- Instance type: t2.micro (free tier)
- Name: retail-intel-streaming
- Key pair: retail-intel-key.pem

### IAM Role — retail-intel-ec2-role
Policies attached:
- AmazonSSMManagedInstanceCore (default)
- CloudWatchAgentServerPolicy (default)
- AmazonS3FullAccess (added manually)

Attach to instance: EC2 Console → Instance → Actions → Security → Modify IAM Role

### Initial Setup (run once after launch)
```bash
sudo dnf update -y
sudo dnf install python3-pip -y
pip3 install boto3 python-dotenv
```

### Copy Script from Local to EC2
Run from your local project root (PowerShell):
```bash
scp -i C:\Users\apokhrel\.ssh\retail-intel-key.pem streaming\clickstream\clickstream_generator.py ec2-user@<your-public-ip>:~/
```

### SSH into EC2
```bash
ssh -i C:\Users\apokhrel\.ssh\retail-intel-key.pem ec2-user@<your-public-ip>
```

### Set S3 Mode
```bash
sed -i 's/LOCAL_MODE = True/LOCAL_MODE = False/' clickstream_generator.py
```

### Run Generator in Background
```bash
nohup python3 -u clickstream_generator.py > generator.log 2>&1 &
echo $! > generator.pid
```

### Monitor Generator
```bash
# Watch live log
tail -f generator.log

# Check if process is running
ps aux | grep clickstream

# Check S3 for new files
aws s3 ls s3://retail-intel-bronze/clickstream/ --recursive
```

### Stop Generator
```bash
kill $(cat generator.pid)
```

### Stop EC2 Instance (to preserve free tier hours)
```bash
aws ec2 stop-instances --instance-ids <your-instance-id>
```
Or via AWS Console → EC2 → Instances → Stop instance.

**Always stop EC2 when not actively using it.**