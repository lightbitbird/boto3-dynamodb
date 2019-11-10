from services.dynamodb import DynamoDB

db = DynamoDB('user_account')

keyStrategy = []
keyStrategy.append({
    'key_type': 'hash',
    'name': 'user_id',
    'type': 'int',
})
keyStrategy.append({
    'key_type': 'range',
    'name': 'user_name',
    'type': 'str',
})
print(db.create_table('user_account', keyStrategy))

item = {
    'user_id': 1,
    'user_name': 'Max',
    'age': 33
}
db.put_item(item)

print(db.get_items())

key = {'user_id': 1,
       'user_name': 'Max'
       }
expressionAttributeNames = {
    '#age': 'age',
    '#level': 'user_level'
}
expressionAttributeValues = {
    ':count': 1,
    ':count2': 5
}
updateExpression = 'ADD #age :count, #level :count2'
returnValues = 'UPDATED_NEW'

print(db.update_item(key, expressionAttributeNames, expressionAttributeValues, updateExpression, returnValues))

print('get_items: ')
print(db.get_items())

key = {
    'user_id': 1,
    'user_name': 'Max'
}
print(db.delete_item(key))

print(db.delete_table())
