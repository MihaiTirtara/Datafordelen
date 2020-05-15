import ijson

events = ijson.basic_parse(open("/home/mehigh/AddressData/AddressAgain.json"))
for prefix,event, value in events:
    if(prefix,event) == ('AdresseList','map_key'):
       print(value)



