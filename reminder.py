from twilio.rest import Client

account_sid = 'AC40483de0d36771e4deb7745725cbc0b5'
auth_token = 'f3ebd75fd9317c87bab732accd4827fd'
client = Client(account_sid, auth_token)

file1 = open('cc1_reminder_list.csv', 'r')
Lines = file1.readlines()
for line in Lines:
    line = line.strip().strip(',')
    print(line)
    message = client.messages.create(
        from_='whatsapp:+12062105220',
        body='Já respondeu o questionário de hoje? Por favor, não se esqueça de completar no link acima',
        to='whatsapp:+' + str(line)
    )
file2 = open('cc2_reminder_list.csv', 'r')
Lines = file2.readlines()
for line in Lines:
    line = line.strip().strip(',')
    print(line)
    message = client.messages.create(
        from_='whatsapp:+12062105220',
        body='Já respondeu o questionário de hoje? Por favor, não se esqueça de completar no link acima',
        to='whatsapp:+' + str(line)
    )
file3 = open('index_reminder_list.csv', 'r')
Lines = file3.readlines()
for line in Lines:
    line = line.strip().strip(',')
    print(line)
    message = client.messages.create(
        from_='whatsapp:+12062105220',
        body='Já respondeu o questionário de hoje? Por favor, não se esqueça de completar no link acima',
        to='whatsapp:+' + str(line)
    )
