{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::video-games/source_input/product/*",
                "arn:aws:s3:::video-games/scripts/*",
                "arn:aws:s3:::video-games/source_output/parquet_product/*"
            ]
        }
    ]
}