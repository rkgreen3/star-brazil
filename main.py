from twilio.rest import Client

account_sid = 'enter here'
auth_token = 'enter here'
client = Client(account_sid, auth_token)

file1 = open('cc1_daily_list.csv', 'r')
Lines = file1.readlines()
for line in Lines:
    line = line.strip().strip(',')
    print(line)
    message = client.messages.create(
    from_='whatsapp:+12062105220',
    body='Por favor, responda o questionário do estudo de COVID-19, do qual você participa: https://redcap.iths.org/surveys/?s=3HAEYATCTKA4A7LL',
    to='whatsapp:+' + str(line)
    )
file2 = open('cc2_daily_list.csv', 'r')
Lines = file2.readlines()
for line in Lines:
    line = line.strip().strip(',')
    print(line)
    message = client.messages.create(
    from_='whatsapp:+12062105220',
    body='Por favor, responda o questionário do estudo de COVID-19, do qual você participa: https://redcap.iths.org/surveys/?s=DRK3DK38PRKPDXFM',
    to='whatsapp:+' + str(line)
    )
file3 = open('index_daily_list.csv', 'r')
Lines = file3.readlines()
for line in Lines:
    line = line.strip().strip(',')
    print(line)
    message = client.messages.create(
    from_='whatsapp:+12062105220',
    body='Por favor, responda o questionário do estudo de COVID-19, do qual você participa: https://redcap.iths.org/surveys/?s=TNCWL49NEMT7A3FN',
    to='whatsapp:+' + str(line)
    )
