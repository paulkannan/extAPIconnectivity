class BQUtils:
    def query(self,context,client, query):
        try:
            query_job = client.query(query)
            return query_job.result()
        except:
            context.logger.error("BQ Error: {}".format(query_job.error_result))
            raise