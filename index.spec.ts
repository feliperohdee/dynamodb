import _ from 'lodash';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import Dynamodb, { concatConditionExpression, concatUpdateExpression } from './';

const createItems = (count: number) => {
	return _.times(count, i => {
		return {
			foo: `foo-${i}`,
			gsiPk: `gsi-pk-${i % 2}`,
			gsiSk: `gsi-sk-${i}`,
			lsiSk: `lsi-sk-${i}`,
			sk: `sk-${i}`,
			pk: `pk-${i % 2}`
		};
	});
};

describe('api/libs/dynamodb', () => {
	let dynamodb: Dynamodb;

	beforeAll(() => {
		dynamodb = new Dynamodb({
			accessKeyId: process.env.AWS_ACCESS_KEY || '',
			region: 'us-east-1',
			secretAccessKey: process.env.AWS_SECRET_KEY || '',
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
			],
			schema: { partition: 'pk', sort: 'sk' },
			table: 'simple-img-new-spec'
		});
	});

	describe('concatConditionExpression', () => {
		it('should works', () => {
			expect(concatConditionExpression('a  ', '  b')).toEqual('a AND b');
			expect(concatConditionExpression('a  ', '  OR b')).toEqual('a OR b');
		});
	});

	describe('concatUpdateExpression', () => {
		it('should works', () => {
			expect(concatUpdateExpression('#a = :a,', '')).toEqual('SET #a = :a');
			expect(concatUpdateExpression('#a = :a,', 'b = :b')).toEqual('SET #a = :a, b = :b');
			expect(concatUpdateExpression('SET #a = :a,', 'SET b = :b,c = :c,')).toEqual('SET #a = :a, b = :b, c = :c');
			expect(concatUpdateExpression('SET #a = :a,', 'ADD d SET b = :b,c = :c,')).toEqual('SET #a = :a, b = :b, c = :c ADD d');
		});
	});

	describe('createTable', () => {
		it('should works', async () => {
			const res = await dynamodb.createTable();

			if ('Table' in res) {
				expect(res.Table?.TableName).toEqual('simple-img-new-spec');
			} else if ('TableDescription' in res) {
				expect(res.TableDescription?.TableName).toEqual('simple-img-new-spec');
			} else {
				throw new Error('Table not created');
			}
		});
	});

	describe('batchWrite / batchDelete', () => {
		beforeEach(async () => {
			vi.spyOn(dynamodb, 'fetch');
			vi.spyOn(dynamodb.client, 'send');
		});

		afterAll(async () => {
			await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete({
					pk: 'pk-1'
				})
			]);
		});

		it('should batch write and batch delete', async () => {
			const wroteItems = await dynamodb.batchWrite(createItems(52));

			expect(
				_.every(wroteItems, item => {
					return _.isNumber(item.__ts);
				})
			).toBeTruthy();

			expect(dynamodb.client.send).toHaveBeenCalledTimes(3);
			vi.mocked(dynamodb.client.send).mockClear();

			const deleteItems = await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete(
					{
						pk: 'pk-1'
					},
					{
						attributeNames: { '#sk': 'sk' },
						attributeValues: { ':from': 'sk-0', ':to': 'sk-999' },
						expression: '#sk BETWEEN :from AND :to'
					}
				)
			]);

			expect(dynamodb.client.send).toHaveBeenCalledTimes(6);
			expect(dynamodb.fetch).toHaveBeenCalledTimes(2);
			expect(dynamodb.fetch).toHaveBeenCalledWith(
				{
					pk: 'pk-0'
				},
				{
					all: true,
					attributeNames: {},
					attributeValues: {},
					expression: '',
					filterExpression: '',
					index: '',
					onChunk: expect.any(Function),
					prefix: false
				}
			);
			expect(dynamodb.fetch).toHaveBeenCalledWith(
				{
					pk: 'pk-1'
				},
				{
					all: true,
					attributeNames: { '#sk': 'sk' },
					attributeValues: { ':from': 'sk-0', ':to': 'sk-999' },
					expression: '#sk BETWEEN :from AND :to',
					filterExpression: '',
					index: '',
					onChunk: expect.any(Function),
					prefix: false
				}
			);

			expect(deleteItems[0]).toHaveLength(26);
			expect(deleteItems[1]).toHaveLength(26);

			const res = await Promise.all([
				dynamodb.fetch({
					pk: 'pk-0'
				}),
				dynamodb.fetch({
					pk: 'pk-1'
				})
			]);

			expect(res[0].items).toHaveLength(0);
			expect(res[1].items).toHaveLength(0);
		});
	});

	describe('delete', () => {
		beforeEach(async () => {
			await dynamodb.batchWrite(createItems(1));

			vi.spyOn(dynamodb, 'get');
			vi.spyOn(dynamodb.client, 'send');
		});

		afterAll(async () => {
			await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete({
					pk: 'pk-1'
				})
			]);
		});

		it('should return null if item not found', async () => {
			const item = await dynamodb.delete({
				pk: 'pk-0',
				sk: 'sk-100'
			});

			expect(item).toBeNull();
		});

		it('should delete', async () => {
			const item = await dynamodb.delete({
				pk: 'pk-0',
				sk: 'sk-0'
			});

			expect(dynamodb.get).toHaveBeenCalledWith(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: {},
					attributeValues: {},
					conditionExpression: '',
					filterExpression: '',
					index: '',
					prefix: false
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_exists(#__ts) AND #__ts = :__ts)',
						ExpressionAttributeNames: {
							'#__ts': '__ts'
						},
						ExpressionAttributeValues: {
							':__ts': expect.any(Number)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-0'
						},
						ReturnValues: 'ALL_OLD',
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					sk: 'sk-0',
					pk: 'pk-0'
				})
			);
		});

		it('should delete by prefix', async () => {
			const item = await dynamodb.delete(
				{
					pk: 'pk-0',
					sk: 'sk-'
				},
				{
					prefix: true
				}
			);

			expect(dynamodb.get).toHaveBeenCalledWith(
				{
					pk: 'pk-0',
					sk: 'sk-'
				},
				{
					attributeNames: {},
					attributeValues: {},
					conditionExpression: '',
					filterExpression: '',
					index: '',
					prefix: true
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_exists(#__ts) AND #__ts = :__ts)',
						ExpressionAttributeNames: {
							'#__ts': '__ts'
						},
						ExpressionAttributeValues: {
							':__ts': expect.any(Number)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-0'
						},
						ReturnValues: 'ALL_OLD',
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					sk: 'sk-0',
					pk: 'pk-0'
				})
			);
		});

		it('should delete by local secondary index', async () => {
			const item = await dynamodb.delete({
				lsiSk: 'lsi-sk-0',
				pk: 'pk-0'
			});

			expect(dynamodb.get).toHaveBeenCalledWith(
				{
					lsiSk: 'lsi-sk-0',
					pk: 'pk-0'
				},
				{
					attributeNames: {},
					attributeValues: {},
					conditionExpression: '',
					filterExpression: '',
					index: '',
					prefix: false
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_exists(#__ts) AND #__ts = :__ts)',
						ExpressionAttributeNames: {
							'#__ts': '__ts'
						},
						ExpressionAttributeValues: {
							':__ts': expect.any(Number)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-0'
						},
						ReturnValues: 'ALL_OLD',
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					sk: 'sk-0',
					pk: 'pk-0'
				})
			);
		});

		it('should delete by global secondary index', async () => {
			const item = await dynamodb.delete({
				gsiPk: 'gsi-pk-0',
				gsiSk: 'gsi-sk-0'
			});

			expect(dynamodb.get).toHaveBeenCalledWith(
				{
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0'
				},
				{
					attributeNames: {},
					attributeValues: {},
					conditionExpression: '',
					filterExpression: '',
					index: '',
					prefix: false
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_exists(#__ts) AND #__ts = :__ts)',
						ExpressionAttributeNames: {
							'#__ts': '__ts'
						},
						ExpressionAttributeValues: {
							':__ts': expect.any(Number)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-0'
						},
						ReturnValues: 'ALL_OLD',
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					sk: 'sk-0',
					pk: 'pk-0'
				})
			);
		});
	});

	describe('fetch', () => {
		beforeAll(async () => {
			await dynamodb.batchWrite(createItems(10));
		});

		afterAll(async () => {
			await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete({
					pk: 'pk-1'
				})
			]);
		});

		beforeEach(() => {
			vi.spyOn(dynamodb.client, 'send');
		});

		it('should fetch with partition', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch({
				pk: 'pk-0'
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ExpressionAttributeNames: {
							'#partition': 'pk'
						},
						ExpressionAttributeValues: {
							':partition': 'pk-0'
						},
						KeyConditionExpression: '#partition = :partition',
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(5);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch by partition/sort', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch({
				pk: 'pk-0',
				sk: 'sk-0'
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ExpressionAttributeNames: {
							'#partition': 'pk',
							'#sort': 'sk'
						},
						ExpressionAttributeValues: {
							':partition': 'pk-0',
							':sort': 'sk-0'
						},
						KeyConditionExpression: '#partition = :partition AND #sort = :sort',
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch by partition/sort with prefix', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch(
				{
					pk: 'pk-0',
					sk: 'sk-'
				},
				{
					prefix: true
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ExpressionAttributeNames: {
							'#partition': 'pk',
							'#sort': 'sk'
						},
						ExpressionAttributeValues: {
							':partition': 'pk-0',
							':sort': 'sk-'
						},
						KeyConditionExpression: '#partition = :partition AND begins_with(#sort, :sort)',
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(5);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch by local secondary index', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch({
				pk: 'pk-0',
				lsiSk: 'lsi-sk-0'
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ExpressionAttributeNames: {
							'#partition': 'pk',
							'#sort': 'lsiSk'
						},
						ExpressionAttributeValues: {
							':partition': 'pk-0',
							':sort': 'lsi-sk-0'
						},
						IndexName: 'ls-index',
						KeyConditionExpression: '#partition = :partition AND #sort = :sort',
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch by global secondary index', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch({
				gsiPk: 'gsi-pk-0',
				gsiSk: 'gsi-sk-0'
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ExpressionAttributeNames: {
							'#partition': 'gsiPk',
							'#sort': 'gsiSk'
						},
						ExpressionAttributeValues: {
							':partition': 'gsi-pk-0',
							':sort': 'gsi-sk-0'
						},
						IndexName: 'gs-index',
						KeyConditionExpression: '#partition = :partition AND #sort = :sort',
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch by custom expression', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch(
				{
					pk: 'pk-0'
				},
				{
					attributeNames: { '#lsiSk': 'lsiSk' },
					attributeValues: { ':from': 'lsi-sk-0', ':to': 'lsi-sk-3' },
					index: 'ls-index',
					expression: ' #lsiSk BETWEEN :from AND :to'
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ExpressionAttributeNames: {
							'#partition': 'pk',
							'#lsiSk': 'lsiSk'
						},
						ExpressionAttributeValues: {
							':partition': 'pk-0',
							':from': 'lsi-sk-0',
							':to': 'lsi-sk-3'
						},
						IndexName: 'ls-index',
						KeyConditionExpression: '#partition = :partition AND #lsiSk BETWEEN :from AND :to',
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(2);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch by filterExpression', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch(
				{
					pk: 'pk-0'
				},
				{
					attributeNames: { '#foo': 'foo' },
					attributeValues: { ':foo': 'foo-0' },
					filterExpression: '#foo = :foo'
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ExpressionAttributeNames: {
							'#partition': 'pk',
							'#foo': 'foo'
						},
						ExpressionAttributeValues: {
							':partition': 'pk-0',
							':foo': 'foo-0'
						},
						FilterExpression: '#foo = :foo',
						KeyConditionExpression: '#partition = :partition',
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toBeNull();
		});

		it('should fetch with limit/startKey', async () => {
			const { count, lastEvaluatedKey } = await dynamodb.fetch(
				{
					pk: 'pk-0'
				},
				{
					limit: 1
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ExpressionAttributeNames: {
							'#partition': 'pk'
						},
						ExpressionAttributeValues: {
							':partition': 'pk-0'
						},
						KeyConditionExpression: '#partition = :partition',
						Limit: 1,
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count).toEqual(1);
			expect(lastEvaluatedKey).toEqual({ pk: 'pk-0', sk: 'sk-0' });

			const { count: count2, lastEvaluatedKey: lastEvaluatedKey2 } = await dynamodb.fetch(
				{
					pk: 'pk-0'
				},
				{
					startKey: lastEvaluatedKey
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ExpressionAttributeNames: {
							'#partition': 'pk'
						},
						ExpressionAttributeValues: {
							':partition': 'pk-0'
						},
						ExclusiveStartKey: { pk: 'pk-0', sk: 'sk-0' },
						KeyConditionExpression: '#partition = :partition',
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(count2).toEqual(4);
			expect(lastEvaluatedKey2).toBeNull();
		});

		it('should fetch all with limit/startKey and onChunk', async () => {
			const onChunk = vi.fn();
			const { count, lastEvaluatedKey } = await dynamodb.fetch(
				{
					pk: 'pk-0'
				},
				{
					all: true,
					onChunk,
					limit: 2
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledTimes(3);
			expect(onChunk).toHaveBeenCalledTimes(3);
			expect(onChunk).toHaveBeenCalledWith({
				count: 2,
				items: expect.any(Array)
			});
			expect(onChunk).toHaveBeenCalledWith({
				count: 1,
				items: expect.any(Array)
			});

			expect(count).toEqual(5);
			expect(lastEvaluatedKey).toBeNull();
		});
	});

	describe('get', () => {
		beforeAll(async () => {
			await dynamodb.batchWrite(createItems(1));
		});

		afterAll(async () => {
			await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete({
					pk: 'pk-1'
				})
			]);
		});

		beforeEach(() => {
			vi.spyOn(dynamodb, 'fetch');
		});

		it('should return null if not found', async () => {
			const item = await dynamodb.get({
				pk: 'pk-0',
				sk: 'sk-100'
			});

			expect(item).toBeNull();
		});

		it('should get', async () => {
			const item = await dynamodb.get({
				pk: 'pk-0',
				sk: 'sk-0'
			});

			expect(dynamodb.fetch).toHaveBeenCalledWith(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: {},
					attributeValues: {},
					filterExpression: '',
					index: '',
					limit: 1,
					prefix: false
				}
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);
		});

		it('should get with options', async () => {
			const item = await dynamodb.get(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: { '#foo': 'foo' },
					attributeValues: { ':foo': 'foo-0' },
					filterExpression: '#foo = :foo'
				}
			);

			expect(dynamodb.fetch).toHaveBeenCalledWith(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: { '#foo': 'foo' },
					attributeValues: { ':foo': 'foo-0' },
					filterExpression: '#foo = :foo',
					index: '',
					limit: 1,
					prefix: false
				}
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);
		});
	});

	describe('put', () => {
		beforeEach(() => {
			vi.spyOn(dynamodb.client, 'send');
		});

		afterAll(async () => {
			await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete({
					pk: 'pk-1'
				})
			]);
		});

		it('should put', async () => {
			const item = await dynamodb.put({
				sk: 'sk-0',
				pk: 'pk-0'
			});

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_not_exists(#partition))',
						ExpressionAttributeNames: { '#partition': 'pk' },
						Item: {
							__ts: expect.any(Number),
							pk: 'pk-0',
							sk: 'sk-0'
						},
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(item).toEqual(
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-0'
				})
			);
		});

		it('should throw on ovewrite', async () => {
			try {
				await dynamodb.put({
					pk: 'pk-0',
					sk: 'sk-0'
				});

				throw new Error('expected to throw');
			} catch (err) {
				expect((err as Error).name).toEqual('ConditionalCheckFailedException');
			}
		});

		it('should put ovewriting', async () => {
			const item = await dynamodb.get({
				pk: 'pk-0',
				sk: 'sk-0'
			});

			const putItem = await dynamodb.put(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					overwrite: true
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						Item: {
							__ts: expect.any(Number),
							pk: 'pk-0',
							sk: 'sk-0'
						},
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(putItem.__ts).toBeGreaterThan(item!.__ts);
			expect(putItem).toEqual(
				expect.objectContaining({
					sk: 'sk-0',
					pk: 'pk-0'
				})
			);
		});

		it('should put with options', async () => {
			const item = await dynamodb.put(
				{
					pk: 'pk-0',
					sk: 'sk-1'
				},
				{
					attributeNames: { '#foo': 'foo' },
					attributeValues: { ':foo': 'foo-0' },
					conditionExpression: '#foo <> :foo',
					overwrite: false
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_not_exists(#partition)) AND #foo <> :foo',
						ExpressionAttributeNames: { '#foo': 'foo', '#partition': 'pk' },
						ExpressionAttributeValues: { ':foo': 'foo-0' },
						Item: {
							__ts: expect.any(Number),
							pk: 'pk-0',
							sk: 'sk-1'
						},
						TableName: 'simple-img-new-spec'
					})
				})
			);

			expect(item).toEqual(
				expect.objectContaining({
					pk: 'pk-0',
					sk: 'sk-1'
				})
			);
		});
	});

	describe('optimisticResolveSchema', () => {
		it('should resolve', () => {
			const { index, schema } = dynamodb.optimisticResolveSchema({
				pk: 'pk-0',
				sk: 'sk-0'
			});

			expect(index).toEqual('sort');
			expect(schema).toEqual({
				partition: 'pk',
				sort: 'sk'
			});
		});

		it('should resolve by local secondary index', () => {
			const { index, schema } = dynamodb.optimisticResolveSchema({
				pk: 'pk-0',
				lsiSk: 'lsi-sk-0'
			});

			expect(index).toEqual('ls-index');
			expect(schema).toEqual({
				partition: 'pk',
				sort: 'lsiSk'
			});
		});

		it('should resolve by global secondary index', () => {
			const { index, schema } = dynamodb.optimisticResolveSchema({
				gsiPk: 'gsi-pk-0',
				gsiSk: 'gsi-sk-0'
			});

			expect(index).toEqual('gs-index');
			expect(schema).toEqual({
				partition: 'gsiPk',
				sort: 'gsiSk'
			});
		});
	});

	describe('update', () => {
		beforeEach(async () => {
			await dynamodb.batchWrite(createItems(1));

			vi.spyOn(dynamodb, 'put');
			vi.spyOn(dynamodb.client, 'send');
		});

		afterEach(async () => {
			await Promise.all([
				dynamodb.batchDelete({
					pk: 'pk-0'
				}),
				dynamodb.batchDelete({
					pk: 'pk-1'
				})
			]);
		});

		it('should throw if item not found', async () => {
			try {
				await dynamodb.update(
					{
						pk: 'pk-0',
						sk: 'sk-1'
					},
					{
						updateFn: item => {
							return {
								...item,
								foo: 'foo-1'
							};
						}
					}
				);

				throw new Error('expected to throw');
			} catch (err) {
				expect((err as Error).message).toEqual('Item not found');
			}
		});

		it('should update', async () => {
			const item = await dynamodb.update(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					updateFn: item => {
						return {
							...item,
							foo: 'foo-1'
						};
					}
				}
			);

			expect(dynamodb.put).toHaveBeenCalledWith(
				{
					__ts: expect.any(Number),
					foo: 'foo-1',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: { '#__ts': '__ts' },
					attributeValues: { ':__ts': expect.any(Number) },
					conditionExpression: '(attribute_not_exists(#__ts) OR #__ts = :__ts)',
					overwrite: true
				}
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-1',
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					sk: 'sk-0',
					pk: 'pk-0'
				})
			);
		});

		it('should upsert', async () => {
			const item = await dynamodb.update(
				{
					pk: 'pk-0',
					sk: 'sk-1'
				},
				{
					updateFn: item => {
						return {
							...item,
							foo: 'foo-1'
						};
					},
					upsert: true
				}
			);

			expect(dynamodb.put).toHaveBeenCalledWith(
				{
					foo: 'foo-1',
					pk: 'pk-0',
					sk: 'sk-1'
				},
				{
					attributeNames: { '#__ts': '__ts' },
					attributeValues: { ':__ts': expect.any(Number) },
					conditionExpression: '(attribute_not_exists(#__ts) OR #__ts = :__ts)',
					overwrite: true
				}
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-1',
					pk: 'pk-0',
					sk: 'sk-1'
				})
			);
		});

		it('should update with expression', async () => {
			const item = await dynamodb.update(
				{
					pk: 'pk-0',
					sk: 'sk-0'
				},
				{
					attributeNames: {
						'#foo': 'foo',
						'#bar': 'bar'
					},
					attributeValues: {
						':foo': 'foo-1',
						':one': 1
					},
					expression: 'SET #foo = if_not_exists(#foo, :foo) ADD #bar :one'
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_not_exists(#__ts) OR #__ts = :__ts)',
						ExpressionAttributeNames: {
							'#bar': 'bar',
							'#foo': 'foo',
							'#__ts': '__ts'
						},
						ExpressionAttributeValues: {
							':foo': 'foo-1',
							':one': 1,
							':__ts': expect.any(Number)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-0'
						},
						ReturnValues: 'ALL_NEW',
						TableName: 'simple-img-new-spec',
						UpdateExpression: 'SET #foo = if_not_exists(#foo, :foo), #__ts = :__ts ADD #bar :one'
					})
				})
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-0',
					bar: 1,
					gsiPk: 'gsi-pk-0',
					gsiSk: 'gsi-sk-0',
					lsiSk: 'lsi-sk-0',
					sk: 'sk-0',
					pk: 'pk-0'
				})
			);
		});

		it('should upsert with expression', async () => {
			const item = await dynamodb.update(
				{
					pk: 'pk-0',
					sk: 'sk-1'
				},
				{
					attributeNames: {
						'#foo': 'foo',
						'#bar': 'bar'
					},
					attributeValues: {
						':foo': 'foo-1',
						':one': 1
					},
					expression: 'SET #foo = if_not_exists(#foo, :foo) ADD #bar :one',
					upsert: true
				}
			);

			expect(dynamodb.client.send).toHaveBeenCalledWith(
				expect.objectContaining({
					input: expect.objectContaining({
						ConditionExpression: '(attribute_not_exists(#__ts) OR #__ts = :__ts)',
						ExpressionAttributeNames: {
							'#bar': 'bar',
							'#foo': 'foo',
							'#__ts': '__ts'
						},
						ExpressionAttributeValues: {
							':foo': 'foo-1',
							':one': 1,
							':__ts': expect.any(Number)
						},
						Key: {
							pk: 'pk-0',
							sk: 'sk-1'
						},
						ReturnValues: 'ALL_NEW',
						TableName: 'simple-img-new-spec',
						UpdateExpression: 'SET #foo = if_not_exists(#foo, :foo), #__ts = :__ts ADD #bar :one'
					})
				})
			);

			expect(item).toEqual(
				expect.objectContaining({
					foo: 'foo-1',
					bar: 1,
					sk: 'sk-1',
					pk: 'pk-0'
				})
			);
		});
	});
});
