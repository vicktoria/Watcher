import boto.ec2
conn = boto.ec2.connect_to_region("us-west-2",
aws_access_key_id='<aws access key>',
aws_secret_access_key='<aws secret key>')

conn.run_instances('<ami-image-id>')
# do some processing

reservations = conn.get_all_instances()
my_instance_ids = reservations.instances
conn.stop_instances(instance_ids=my_instance_ids)

