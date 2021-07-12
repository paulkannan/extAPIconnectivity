import paramiko
import sys, traceback


class SFTPpush:
    def sftp_push(self, context, file, password):
        try:
            context.logger.info('Starting -- SFTP send....')
            hostname = context.settings.get('HOST_NAME')
            port = context.settings.get('PORT')
            username = context.settings.get('USER_NAME')
            
            proxy = context.settings.get('PROXY')
            paramiko.util.log_to_file('/tmp/paramiko.log')
            proxy = paramiko.proxy.ProxyCommand('/usr/bin/nc --proxy %s %s %s' %(proxy, hostname, port))

            #Open a transport
            transport = paramiko.Transport(sock=proxy)

            #Auth
            transport.connect(None, username, password)
            context.logger.info("Transport Connected: "+str(transport.is_active()))

            #Go
            sftp = paramiko.SFTPClient.from_transport(transport)

            #Upload
            result = sftp.put(file, file.split('.')[-1], confirm=False)
            context.logger.info(result)

        except Exception as e:
            print('Error in SFTP Push: {}'.format(sys.exc_info()[1]))
            context.logger.error("Error in SFTP Push".format(traceback.format_exc()))

        finally:
            if sftp: sftp.close()
            if transport: transport.close()
            context.logger.info('End')