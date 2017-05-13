import boto
import boto.s3
import logging
import os.path
import sys
import ConfigParser

class upload_to_s3(object):

    def __init__(self, sourceDir, destDir, bucket_name):
        self.sourceDir=sourceDir
        self.destDir=destDir
        self.bucket_name=bucket_name

    def percent_cb(self, complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()

    #Upload all the files within the source directory to S3
    def upload_dir(self):
        for (local_Dir, dirname, files) in os.walk(self.sourceDir):
            for filename in files:
                sourcepath = os.path.join(local_Dir, filename)
                destpath = os.path.join(self.destDir, filename)
                logging.debug ("Uploading %s to Amazon S3 bucket %s" % (sourcepath, self.bucket_name))

                filesize = os.path.getsize(sourcepath)
                if filesize > MAX_SIZE:
                    # Upload big files in multiple parts
                    logging.debug ("multipart upload")
                    mp = bucket.initiate_multipart_upload(destpath)
                    fp = open(sourcepath,'rb')                        
                    fp_num = 0
                    while (fp.tell() < filesize):
                        fp_num += 1
                        logging.debug ("uploading part %i" %fp_num)
                        mp.upload_part_from_file(fp, fp_num, cb=self.percent_cb, num_cb=10, size=PART_SIZE)

                    mp.complete_upload()

                else:
                    # Upload small files
                    logging.debug("singlepart upload")
                    k = boto.s3.key.Key(bucket)
                    k.key = destpath
                    k.set_contents_from_filename(sourcepath,
                        cb=self.percent_cb, num_cb=10)


if __name__ == "__main__":
        logger=logging.basicConfig(filename='UploadingToS3.log', level=logging.DEBUG)
        try:
            Config = ConfigParser.RawConfigParser()
            Config.read("application.cfg")
            # specify source directory and destination directory name (on s3)
            sourceDir = Config.get("SectionOne", "sourceDir")
            destDir = Config.get("SectionOne", "destDir")
            bucket_name = Config.get("SectionOne", "Bucket")
            AWS_ACCESS_KEY_ID = Config.get("SectionOne", "AWS_ACCESS_KEY_ID")
            AWS_ACCESS_KEY_SECRET = Config.get("SectionOne", "AWS_ACCESS_KEY_SECRET")

            #max size in bytes before uploading in parts. between 1 and 5 GB recommended
            MAX_SIZE = Config.getint("SectionOne", "MAX_SIZE")
            #size of parts when uploading in parts
            PART_SIZE = Config.getint("SectionOne", "PART_SIZE")

        except:
            logging.error("No configuration file is found.")
            raise

        #Build the connection to S3
        try:
            conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET)
            bucket = conn.get_bucket(bucket_name)
            uploader=upload_to_s3(sourceDir, destDir, bucket_name)
            uploader.upload_dir()
        except:
            logging.error("Connection was not built successfully with S3")
            raise
