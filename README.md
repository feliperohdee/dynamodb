# DynamoDB Wrapper

A TypeScript library that provides a simplified interface for interacting with Amazon DynamoDB, using the AWS SDK v3.

## Features

- Support for CRUD operations (Create, Read, Update, Delete)
- Support for local and global secondary indexes
- Batch operations (batch write and delete)
- Optimized queries with filtering
- Conditional updates
- Pagination support
- Upsert support
- Automatic timestamp management

## Installation

```bash
yarn add @simpleimg/dynamodb
```

## Usage

### Initialization

```typescript
import Dynamodb from '@simpleimg/dynamodb';

const dynamodb = new Dynamodb({
	accessKeyId: 'YOUR_ACCESS_KEY',
	secretAccessKey: 'YOUR_SECRET_KEY',
	region: 'us-east-1',
	table: 'YOUR_TABLE_NAME',
	schema: { partition: 'pk', sort: 'sk' },
	indexes: [
		{
			name: 'ls-index',
			partition: 'pk',
			sort: 'lsiSk',
			type: 'S'
		},
		{
			name: 'gs-index',
			partition: 'gsiPk',
			sort: 'gsiSk',
			type: 'S'
		}
	]
});
```

### Basic Operations

#### Create/Update Item (Put)

```typescript
const item = await dynamodb.put({
	pk: 'user#123',
	sk: 'profile',
	name: 'John Doe',
	email: 'john@example.com'
});
```

#### Get Item

```typescript
const item = await dynamodb.get({
	pk: 'user#123',
	sk: 'profile'
});
```

#### Update Item

```typescript
const updatedItem = await dynamodb.update(
	{
		pk: 'user#123',
		sk: 'profile'
	},
	{
		updateFn: item => ({
			...item,
			email: 'newemail@example.com'
		})
	}
);
```

#### Update Item with Expression

```typescript
const updatedItem = await dynamodb.update(
	{
		pk: 'user#123',
		sk: 'profile'
	},
	{
		expression: 'SET #email = :email, #lastUpdated = :now',
		attributeNames: {
			'#email': 'email',
			'#lastUpdated': 'lastUpdated'
		},
		attributeValues: {
			':email': 'newemail@example.com',
			':now': new Date().toISOString()
		}
	}
);
```

#### Delete Item

```typescript
const deletedItem = await dynamodb.delete({
	pk: 'user#123',
	sk: 'profile'
});
```

### Advanced Query Operations

#### Fetch (Query) Items

Basic fetch:

```typescript
const { items, count, lastEvaluatedKey } = await dynamodb.fetch({
	pk: 'user#123'
});
```

#### Fetch with Filter

```typescript
const { items, count, lastEvaluatedKey } = await dynamodb.fetch(
	{ pk: 'user#123' },
	{
		attributeNames: { '#foo': 'foo' },
		attributeValues: { ':foo': 'foo-0' },
		filterExpression: '#foo = :foo'
	}
);
```

#### Fetch with Pagination

```typescript
const { items, count, lastEvaluatedKey } = await dynamodb.fetch(
	{ pk: 'user#123' },
	{
		limit: 10,
		startKey: lastEvaluatedKey // from previous query
	}
);
```

#### Fetch All Items with Chunking

```typescript
const { items, count, lastEvaluatedKey } = await dynamodb.fetch(
	{ pk: 'user#123' },
	{
		all: true,
		limit: 10,
		onChunk: ({ items, count }) => {
			console.log(`Received ${count} items in this chunk`);
			// Process items here
		}
	}
);
```

#### Get with Filter

```typescript
const item = await dynamodb.get(
	{ pk: 'user#123', sk: 'profile' },
	{
		attributeNames: { '#foo': 'foo' },
		attributeValues: { ':foo': 'foo-0' },
		filterExpression: '#foo = :foo'
	}
);
```

#### Delete with Condition

```typescript
const deletedItem = await dynamodb.delete(
	{ pk: 'user#123', sk: 'profile' },
	{
		conditionExpression: 'attribute_exists(email)'
	}
);
```

### Batch Operations

#### Batch Write

```typescript
const items = [
	{ pk: 'user#1', sk: 'profile', name: 'User 1' },
	{ pk: 'user#2', sk: 'profile', name: 'User 2' }
];
await dynamodb.batchWrite(items);
```

#### Batch Delete

Basic batch delete:

```typescript
await dynamodb.batchDelete({ pk: 'user#123' });
```

Batch delete with filter:

```typescript
await dynamodb.batchDelete(
	{ pk: 'user#123' },
	{
		attributeNames: { '#sk': 'sk' },
		attributeValues: { ':from': 'sk-0', ':to': 'sk-999' },
		expression: '#sk BETWEEN :from AND :to'
	}
);
```

### Using Indexes

#### Query using Local Secondary Index

```typescript
const { items, count, lastEvaluatedKey } = await dynamodb.fetch({
	pk: 'user#123',
	lsiSk: 'lsi-value'
});
```

#### Query using Global Secondary Index

```typescript
const { items, count, lastEvaluatedKey } = await dynamodb.fetch({
	gsiPk: 'gsi-partition-value',
	gsiSk: 'gsi-sort-value'
});
```

## Testing

This library includes a comprehensive set of tests. To run the tests:

1. Set the required environment variables:

```bash
export AWS_REGION='us-east-1'
export AWS_ACCESS_KEY='YOUR_ACCESS_KEY'
export AWS_SECRET_KEY='YOUR_SECRET_KEY'
```

2. Run the tests:

```bash
yarn test
```

Make sure to replace 'YOUR_ACCESS_KEY' and 'YOUR_SECRET_KEY' with your actual AWS credentials.

## Contributing

Contributions are welcome! Please open an issue to discuss proposed changes or submit a pull request.

## License

This project is licensed under the ISC license.
