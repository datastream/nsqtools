# file_to_nsq config example

## config.json
`
{
    "consul_address":"127.0.0.1:8500",
    "datacenter":"spacex",
    "consul_token":"xxxx,
    "cluster":"filetonsq/clusters/rocket1"
}
`
## consul key

key 'filetonsq/clusters/rocket1/xxxx'
value '/var/log/xxx.log,/var/log/yyy.log:3'

value pattern
`filename1,filename2,....,filenameN:batchnumber`

value '/var/log/xxx.log'

value '/var/log/xxx.log,/var/log/yyy.log'

default batchnumber is 20.
