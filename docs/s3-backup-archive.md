# S3 backup and archive preparation

This repository can upload raw archives and PostgreSQL backups to S3. The operator
reports that the private bucket and EC2 role now exist, but this repository has not
contacted AWS and therefore has not verified either resource, its policies, or its
attachment to the instance.

## Runtime behavior

Production sets `ARCHIVE_BACKEND=s3`. Raw protobufs, static ZIPs, and expired
PostgreSQL history use the same archive interface as local development. A source is
deleted only after S3 upload and `HeadObject` verification of size, SHA-256 checksum,
metadata and explicit SSE-S3 encryption. A failed upload is logged, recorded under
`ARCHIVE_STATUS_PATH`, raised to the retention worker, and leaves the source intact.

The backup command uploads the verified custom-format dump, its SHA-256 sidecar,
then a fixed `latest-success.json` manifest. It prints success only after that final
manifest is verified. Failed uploads leave the local dump for diagnosis/retry and
record failure under `BACKUP_STATUS_PATH`.

Boto3 receives no access key or secret. It uses the standard credential provider
chain and, on EC2, the instance profile role. Require IMDSv2. Because the retention
worker runs inside Docker, set the EC2 metadata response hop limit to 2 and confirm
the container can obtain role credentials before launch. Do not inject long-lived
AWS credentials into `.env.prod`.

Required production settings:

```dotenv
ARCHIVE_BACKEND=s3
S3_BUCKET_NAME=YOUR_PRIVATE_BUCKET
AWS_REGION=us-east-1
S3_RAW_ARCHIVE_PREFIX=raw-archives
S3_POSTGRES_BACKUP_PREFIX=postgres-backups
S3_BACKUP_MAX_AGE_HOURS=26
```

Run the daily backup from the EC2 host:

```bash
python -m scripts.postgres_backup --production backup --output backups
python -m scripts.ops_health_check --production
```

## Exact EC2 role permissions

Replace `YOUR_PRIVATE_BUCKET` and the two prefixes if configured differently. The
application does not list buckets or objects, delete S3 objects, alter lifecycle,
or alter encryption. `HeadObject` authorization uses `s3:GetObject`. Multipart
permissions are required by boto3's transfer manager for large database dumps.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "WriteEncryptedTransitArchives",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR_PRIVATE_BUCKET/raw-archives/*",
        "arn:aws:s3:::YOUR_PRIVATE_BUCKET/postgres-backups/*"
      ],
      "Condition": {
        "StringEquals": {
          "s3:x-amz-server-side-encryption": "AES256"
        }
      }
    },
    {
      "Sid": "ManageTransitMultipartUploads",
      "Effect": "Allow",
      "Action": [
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR_PRIVATE_BUCKET/raw-archives/*",
        "arn:aws:s3:::YOUR_PRIVATE_BUCKET/postgres-backups/*"
      ]
    },
    {
      "Sid": "VerifyTransitArchives",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": [
        "arn:aws:s3:::YOUR_PRIVATE_BUCKET/raw-archives/*",
        "arn:aws:s3:::YOUR_PRIVATE_BUCKET/postgres-backups/*"
      ]
    }
  ]
}
```

EC2 role trust policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "ec2.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
```

Also use a bucket policy to deny non-TLS requests and deny `PutObject` when the
`s3:x-amz-server-side-encryption` request header is not `AES256`. Enable Block Public
Access, bucket default SSE-S3, and versioning as defense in depth. Those controls are
not substitutes for the explicit encryption header sent by the application.

## Recommended lifecycle

Configure these rules manually and review them against RPO, ML-history requirements,
minimum storage durations, retrieval time, and current S3 pricing:

- `postgres-backups/`: retain in S3 Standard for 30 days, transition objects larger
  than 128 KiB to Glacier Flexible Retrieval after 30 days, expire current versions
  after 365 days, and abort incomplete multipart uploads after 7 days.
- `raw-archives/`: retain in Standard for 30 days, transition objects larger than
  128 KiB after 30 days, and expire after 180 days. Small protobuf objects should
  stay Standard until expiration because transition request/minimum-size economics
  can cost more than their storage.
- Both prefixes: expire noncurrent versions after 30 days. The status manifest is
  overwritten, so noncurrent-version cleanup prevents unlimited marker growth.

Lifecycle expiration is irreversible. Preserve a longer raw-history window if the
approved ML retention requirement exceeds 180 days. Test restore from the selected
Glacier class before relying on it.

## Manual AWS verification still required

The bucket and role are reported as created. Before launch, verify rather than recreate:

1. The bucket is in `AWS_REGION`, private, has Block Public Access, default SSE-S3,
   versioning, the lifecycle rules above, and a policy denying non-TLS or unencrypted writes.
2. The existing role has only the prefix-scoped policy above and an EC2 instance profile.
3. Attach that instance profile to the deployment EC2 instance. Require IMDSv2 and set
   the metadata response hop limit to 2 so the Docker workload can use role credentials.
4. From the instance and application container, verify the caller identity and perform a
   real encrypted upload/`HeadObject` check. Do not add access keys to `.env.prod`.
5. Configure a daily host scheduler for the backup command and an off-host alert when
   the backup or operational check exits nonzero.
