import dao.DBManager

def listChannels():
    list_channels = []
    sql = "SELECT * FROM content_channelmetadata"
    list_channels = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI,sql)
    return list_channels