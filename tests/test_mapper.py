# import src.geocloudservice.db.mapper as mapper
import utils.db.mapper as mapper

def test_getIdByStatus():
    result = mapper.getIdByStatus()
    print(result)
    
def test_getDatanameByOrderId():
    idlist = mapper.getIdByStatus()
    for id in idlist:
        result = mapper.getDatanameByOrderId(id[0])
        print(result)
    
test_getDatanameByOrderId()