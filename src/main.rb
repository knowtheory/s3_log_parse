require 'dm-core'
require 'dm-validations'
require 'dm-postgres-adapter'
# This list is derived from http://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html
HEADERS = [
  :bucket_owner,
  :bucket,
  :time,
  :remote_ip,
  :requester,
  :request_id,
  :operation,
  :aws_key,
  :request_uri,
  :status_code,
  :error_code,
  :bytes_sent,
  :object_size,
  :total_time,
  :turn_around_time,
  :referrer,
  :user_agent,
  :version_id
]

DataMapper.setup(:default, "postgres://ted@localhost:5432/dcloud_s3_analytics")

class Entry
  include DataMapper::Resource

  property :id,               Serial
  property :bucket_owner,     String, :length=>255
  property :bucket,           String, :length=>255
  property :time,             DateTime
  property :remote_ip,        String, :length=>255
  property :requester,        String, :length=>255
  property :request_id,       String, :length=>255
  property :operation,        String, :length=>255
  property :aws_key,          String, :length=>2048
  property :request_uri,      String, :length=>2048
  property :status_code,      String
  property :error_code,       String
  property :bytes_sent,       Integer
  property :object_size,      Integer
  property :total_time,       Integer
  property :turn_around_time, Integer
  property :referrer,         String, :length=>2048
  property :user_agent,       String, :length=>2048
  property :version_id,       String, :length=>255
end
DataMapper.finalize


def process(values)
  e = Entry.new Hash[HEADERS.zip(values.map{|v| v=='-' ? nil : v })]
  e.time = DateTime.strptime(e.time, '%d/%b/%Y:%H:%M:%S %z') unless e.time.nil?
  if e.valid?
    e.save
  else
    puts e.errors.inspect
  end
end

log = File.open('/Users/ted/data/dc/aws_usage/access/2014-10-27.log')

log.rewind
tokens = []
token = ''
terminator = ' '
skip_next = false
log.each_char do |c|
  if c == "\n"
    # flush the last token
    tokens.push token
    process(tokens)
    token = ''
    tokens.clear
  elsif skip_next
    # skip this character, it's a space
    skip_next = false
  elsif c == terminator
    tokens.push token
    token = ''
    skip_next = true unless terminator == ' '
    terminator = ' '
  elsif c == '"'
    terminator = '"'
  elsif c == '['
    terminator = ']'
  else
    token << c
  end
end
