import _ from 'lodash';
import {
	BatchWriteCommand,
	DeleteCommand,
	DynamoDBDocumentClient,
	PutCommand,
	PutCommandInput,
	QueryCommand,
	QueryCommandInput,
	UpdateCommand
} from '@aws-sdk/lib-dynamodb';
import {
	AttributeDefinition,
	CreateTableCommand,
	CreateTableCommandOutput,
	DescribeTableCommand,
	DescribeTableCommandOutput,
	DynamoDBClient,
	GlobalSecondaryIndex,
	LocalSecondaryIndex
} from '@aws-sdk/client-dynamodb';

type Dict<T = any> = { [key: string]: T };
type TableSchema = { partition: string; sort: string };
type TableIndex = TableSchema & { name: string; type: 'S' | 'N' };

type SharedOptions = {
	attributeNames?: Dict<string>;
	attributeValues?: Dict<string | number>;
	filterExpression?: string;
	index?: string;
	prefix?: boolean;
};

const BATCH_DELETE_OPTS: SharedOptions & {
	expression?: string;
} = {
	attributeNames: {},
	attributeValues: {},
	expression: '',
	filterExpression: '',
	index: '',
	prefix: false
};

const DELETE_OPTS: SharedOptions & {
	conditionExpression?: string;
} = {
	attributeNames: {},
	attributeValues: {},
	conditionExpression: '',
	filterExpression: '',
	index: '',
	prefix: false
};

const FETCH_OPTS: SharedOptions & {
	all?: boolean;
	expression?: string;
	onChunk?: ({ count, items }: { count: number; items: Dict[] }) => Promise<void> | void;
	limit?: number;
	startKey?: Dict<string> | null;
} = {
	all: false,
	attributeNames: {},
	attributeValues: {},
	expression: '',
	filterExpression: '',
	index: '',
	limit: Infinity,
	onChunk: () => {},
	prefix: false,
	startKey: null
};

const GET_OPTS: SharedOptions = {
	attributeNames: {},
	attributeValues: {},
	filterExpression: '',
	index: '',
	prefix: false
};

const PUT_OPTS: {
	attributeNames?: Dict<string>;
	attributeValues?: Dict<string | number>;
	conditionExpression?: string;
	overwrite: boolean;
} = {
	attributeNames: {},
	attributeValues: {},
	conditionExpression: '',
	overwrite: false
};

const UPDATE_OPTS: SharedOptions & {
	conditionExpression?: string;
	expression?: string;
	updateFn?: (item: Dict) => Dict | null;
	upsert?: boolean;
} = {
	attributeNames: {},
	attributeValues: {},
	conditionExpression: '',
	expression: '',
	filterExpression: '',
	index: '',
	prefix: false,
	updateFn: _.identity,
	upsert: false
};

const CONSTRUCTOR_OPTS: {
	accessKeyId: string;
	indexes?: TableIndex[];
	region: string;
	schema: TableSchema;
	secretAccessKey: string;
	table: string;
} = {
	accessKeyId: '',
	indexes: [],
	region: '',
	schema: { partition: 'namespace', sort: 'id' },
	secretAccessKey: '',
	table: ''
};

const concatConditionExpression = (exp1: string, exp2: string): string => {
	const JOINERS = ['AND', 'OR'];

	// Trim both expressions
	const trimmedExp1 = _.trim(exp1);
	const trimmedExp2 = _.trim(exp2);

	// Check if exp2 starts with a joiner
	const startsWithJoiner = _.some(JOINERS, joiner => {
		return _.startsWith(trimmedExp2, joiner);
	});

	// Concatenate expressions
	const concatenated = startsWithJoiner ? `${trimmedExp1} ${trimmedExp2}` : `${trimmedExp1} AND ${trimmedExp2}`;

	// Remove leading 'AND' or 'OR' and trim
	return _.trim(_.replace(concatenated, /^\s+(AND|OR)\s+/, ''));
};

const concatUpdateExpression = (exp1: string, exp2: string): string => {
	const TRIM = ', ';

	const extractSection = (exp: string, sec: string): string => {
		const regex = new RegExp(`${sec}\\s+([^A-Z]+)(?=[A-Z]|$)`, 'g');
		const match = exp.match(regex);

		return match ? _.trim(match[0], ' ,') : '';
	};

	exp1 = _.trim(exp1, TRIM);
	exp2 = _.trim(exp2, TRIM);

	const sections = ['SET', 'ADD', 'DELETE', 'REMOVE'];
	const parts: { [key: string]: string[] } = {};

	_.forEach(sections, sec => {
		const part1 = extractSection(exp1, sec);
		const part2 = extractSection(exp2, sec);

		if (part1 || part2) {
			const items1 = part1 ? part1.replace(`${sec}`, '').split(',') : [];
			const items2 = part2 ? part2.replace(`${sec}`, '').split(',') : [];

			parts[sec] = _.uniq(
				[...items1, ...items2].map(item => {
					return _.trim(item, TRIM);
				})
			);
		}
	});

	let result = _.trim(
		_.map(sections, sec => {
			if (parts[sec] && parts[sec].length > 0) {
				return `${sec} ${_.join(parts[sec], ', ')}`;
			}
			return '';
		})
			.filter(Boolean)
			.join(' ')
	);

	if (_.isEmpty(result)) {
		const combinedExp = _.trim(`${exp1}, ${exp2}`, TRIM);

		result = combinedExp ? `SET ${combinedExp}` : '';
	}

	return result;
};

class Dynamodb {
	public client: DynamoDBDocumentClient;
	public indexes: TableIndex[];
	public schema: TableSchema;
	public table: string;

	constructor(opts = CONSTRUCTOR_OPTS) {
		opts = _.defaults({}, opts, CONSTRUCTOR_OPTS);

		this.client = DynamoDBDocumentClient.from(
			new DynamoDBClient({
				credentials: {
					accessKeyId: opts.accessKeyId,
					secretAccessKey: opts.secretAccessKey
				},
				region: opts.region
			})
		);

		this.indexes = opts.indexes || [];
		this.schema = opts.schema;
		this.table = opts.table;
	}

	async batchWrite(items: Dict[]): Promise<Dict[]> {
		const ts = _.now();

		items = _.map(items, item => {
			return { ...item, __ts: ts };
		});

		const chunks = _.chunk(items, 25);

		for (const chunk of chunks) {
			await this.client.send(
				new BatchWriteCommand({
					RequestItems: {
						[this.table]: _.map(chunk, item => {
							return {
								PutRequest: { Item: item }
							};
						})
					}
				})
			);
		}

		return items as Dict[];
	}

	async batchDelete(item: Dict, opts = BATCH_DELETE_OPTS): Promise<Dict[]> {
		opts = _.defaults({}, opts, BATCH_DELETE_OPTS);

		const del = async (items: Dict[]) => {
			const chunkItems = _.map(items, item => {
				return _.pick(item, [this.schema.partition, this.schema.sort]);
			});

			const chunks = _.chunk(chunkItems, 25);

			for (const chunk of chunks) {
				await this.client.send(
					new BatchWriteCommand({
						RequestItems: {
							[this.table]: _.map(chunk, item => {
								return {
									DeleteRequest: {
										Key: {
											[this.schema.partition]: item[this.schema.partition],
											[this.schema.sort]: item[this.schema.sort]
										}
									}
								};
							})
						}
					})
				);
			}
		};

		const { items } = await this.fetch(item, {
			...opts,
			all: true,
			onChunk: async ({ items }) => {
				await del(items);
			}
		});

		return items;
	}

	async delete(item: Dict, opts = DELETE_OPTS): Promise<Dict | null> {
		opts = _.defaults({}, opts, DELETE_OPTS);

		const current = await this.get(item, opts);

		if (!current) {
			return null;
		}

		let conditionExpression = '(attribute_exists(#__ts) AND #__ts = :__ts)';

		if (opts.conditionExpression) {
			conditionExpression = concatConditionExpression(conditionExpression, opts.conditionExpression);
		}

		const res = await this.client.send(
			new DeleteCommand({
				ConditionExpression: conditionExpression,
				ExpressionAttributeNames: { ...opts.attributeNames, '#__ts': '__ts' },
				ExpressionAttributeValues: { ...opts.attributeValues, ':__ts': current.__ts },
				Key: {
					[this.schema.partition]: current[this.schema.partition],
					[this.schema.sort]: current[this.schema.sort]
				},
				ReturnValues: 'ALL_OLD',
				TableName: this.table
			})
		);

		return res.Attributes as Dict;
	}

	async fetch(
		item: Dict,
		opts = FETCH_OPTS
	): Promise<{
		count: number;
		items: Dict[];
		lastEvaluatedKey: Dict<string> | null;
	}> {
		opts = _.defaults({}, opts, FETCH_OPTS);

		let queryParams: QueryCommandInput = {
			ExpressionAttributeNames: {},
			ExpressionAttributeValues: {},
			KeyConditionExpression: '#partition = :partition',
			TableName: this.table
		};

		if (opts.limit && opts.limit !== Infinity) {
			queryParams.Limit = opts.limit;
		}

		if (opts.startKey) {
			queryParams.ExclusiveStartKey = opts.startKey;
		}

		let { index, schema } = this.optimisticResolveSchema(item);

		if (opts.index) {
			queryParams.IndexName = opts.index;
		}

		if (index) {
			if (index !== 'sort' && !queryParams.IndexName) {
				queryParams.IndexName = index;
			}

			if (opts.prefix) {
				queryParams.KeyConditionExpression += ' AND begins_with(#sort, :sort)';
			} else {
				queryParams.KeyConditionExpression += ' AND #sort = :sort';
			}

			queryParams = {
				...queryParams,
				ExpressionAttributeNames: {
					...queryParams.ExpressionAttributeNames,
					'#partition': schema.partition,
					'#sort': schema.sort
				},
				ExpressionAttributeValues: {
					...queryParams.ExpressionAttributeValues,
					':partition': item[schema.partition],
					':sort': item[schema.sort]
				}
			};
		} else {
			queryParams = {
				...queryParams,
				ExpressionAttributeNames: {
					...queryParams.ExpressionAttributeNames,
					'#partition': this.schema.partition
				},
				ExpressionAttributeValues: {
					...queryParams.ExpressionAttributeValues,
					':partition': item[this.schema.partition]
				}
			};
		}

		if (_.size(opts.attributeNames)) {
			queryParams.ExpressionAttributeNames = {
				...queryParams.ExpressionAttributeNames,
				...opts.attributeNames
			};
		}

		if (_.size(opts.attributeValues)) {
			queryParams.ExpressionAttributeValues = {
				...queryParams.ExpressionAttributeValues,
				...opts.attributeValues
			};
		}

		if (opts.expression) {
			queryParams.KeyConditionExpression = concatConditionExpression(queryParams.KeyConditionExpression || '', opts.expression);
		}

		if (opts.filterExpression) {
			queryParams.FilterExpression = opts.filterExpression;
		}

		let res = await this.client.send(new QueryCommand(queryParams));
		let items = res.Items || [];

		if (_.isFunction(opts.onChunk)) {
			await opts.onChunk({
				count: _.size(items),
				items
			});
		}

		if (opts.all) {
			while (res.LastEvaluatedKey) {
				res = await this.client.send(
					new QueryCommand({
						...queryParams,
						ExclusiveStartKey: res.LastEvaluatedKey
					})
				);

				if (_.isFunction(opts.onChunk)) {
					opts.onChunk({
						count: _.size(res.Items),
						items: res.Items || []
					});
				}

				if (res.Items) {
					items = [...items, ...res.Items];
				}
			}
		}

		return {
			count: _.size(items),
			items: items as Dict[],
			lastEvaluatedKey: res.LastEvaluatedKey || null
		};
	}

	async get(item: Dict, opts = GET_OPTS): Promise<Dict | null> {
		opts = _.defaults({}, opts, GET_OPTS);

		const res = await this.fetch(item, {
			...opts,
			limit: 1
		});

		return _.size(res.items) > 0 ? res.items[0] : null;
	}

	async put(item: Dict, opts = PUT_OPTS): Promise<Dict> {
		opts = _.defaults({}, opts, PUT_OPTS);

		let conditionExpression = '';

		if (!opts.overwrite) {
			conditionExpression = '(attribute_not_exists(#partition))';
			opts.attributeNames = {
				...opts.attributeNames,
				'#partition': this.schema.partition
			};
		}

		if (opts.conditionExpression) {
			conditionExpression = concatConditionExpression(conditionExpression, opts.conditionExpression);
		}

		item = { ...item, __ts: _.now() };

		const putParams: PutCommandInput = {
			Item: item,
			TableName: this.table
		};

		if (conditionExpression) {
			putParams.ConditionExpression = conditionExpression;
		}

		if (_.size(opts.attributeNames)) {
			putParams.ExpressionAttributeNames = opts.attributeNames;
		}

		if (_.size(opts.attributeValues)) {
			putParams.ExpressionAttributeValues = opts.attributeValues;
		}

		await this.client.send(new PutCommand(putParams));

		return item as Dict;
	}

	optimisticResolveSchema(item: Dict): { index: string; schema: TableSchema } {
		// test if has partition and sort keys
		if (_.has(item, this.schema.partition) && _.has(item, this.schema.sort)) {
			return {
				index: 'sort',
				schema: {
					partition: this.schema.partition,
					sort: this.schema.sort
				}
			};
		}

		// test if match any index's schema
		for (const { name, partition, sort } of this.indexes) {
			if (_.has(item, partition) && _.has(item, sort)) {
				return { index: name, schema: { partition, sort } };
			}
		}

		return { index: '', schema: { partition: '', sort: '' } };
	}

	async update(item: Dict, opts = UPDATE_OPTS): Promise<Dict> {
		opts = _.defaults({}, opts, UPDATE_OPTS);

		let current = await this.get(item);

		if (!current && !opts.upsert) {
			throw new Error('Item not found');
		}

		let conditionExpression = '(attribute_not_exists(#__ts) OR #__ts = :__ts)';

		if (opts.conditionExpression) {
			conditionExpression = concatConditionExpression(conditionExpression, opts.conditionExpression);
		}

		if (opts.expression) {
			opts.expression = concatUpdateExpression(opts.expression, 'SET #__ts = :__ts');
			opts.attributeNames = {
				...opts.attributeNames,
				'#__ts': '__ts'
			};

			opts.attributeValues = {
				...opts.attributeValues,
				':__ts': current?.__ts || _.now()
			};

			const res = await this.client.send(
				new UpdateCommand({
					ConditionExpression: conditionExpression,
					ExpressionAttributeNames: opts.attributeNames,
					ExpressionAttributeValues: opts.attributeValues,
					Key: {
						[this.schema.partition]: item[this.schema.partition],
						[this.schema.sort]: item[this.schema.sort]
					},
					ReturnValues: 'ALL_NEW',
					TableName: this.table,
					UpdateExpression: opts.expression
				})
			);

			return res.Attributes as Dict;
		}

		if (_.isFunction(opts.updateFn)) {
			const updateFnRes = opts.updateFn(_.isNil(current) ? item : current);

			if (!_.isNil(updateFnRes)) {
				item = updateFnRes;
			}
		}

		return this.put(item, {
			attributeNames: { ...opts.attributeNames, '#__ts': '__ts' },
			attributeValues: { ...opts.attributeValues, ':__ts': current?.__ts || _.now() },
			conditionExpression,
			overwrite: true
		});
	}

	async createTable(): Promise<DescribeTableCommandOutput | CreateTableCommandOutput> {
		try {
			return await this.client.send(
				new DescribeTableCommand({
					TableName: this.table
				})
			);
		} catch (err) {
			const inexistentTable = (err as Error).message.includes('resource not found');

			if (inexistentTable) {
				const gi = _.filter(this.indexes, index => {
					return index.partition !== this.schema.partition;
				});

				const globalIndexes = _.map(gi, index => {
					return {
						IndexName: index.name,
						KeySchema: [
							{
								AttributeName: index.partition,
								KeyType: 'HASH'
							},
							{
								AttributeName: index.sort,
								KeyType: 'RANGE'
							}
						],
						Projection: {
							ProjectionType: 'ALL'
						}
					};
				}) as GlobalSecondaryIndex[];

				const globalIndexesDefinitions = _.flatMap(gi, index => {
					return [
						{
							AttributeName: index.partition,
							AttributeType: 'S'
						},
						{
							AttributeName: index.sort,
							AttributeType: index.type
						}
					];
				}) as AttributeDefinition[];

				const li = _.filter(this.indexes, index => {
					return index.partition === this.schema.partition;
				});

				const localIndexes = _.map(li, index => {
					return {
						IndexName: index.name,
						KeySchema: [
							{
								AttributeName: this.schema.partition,
								KeyType: 'HASH'
							},
							{
								AttributeName: index.sort,
								KeyType: 'RANGE'
							}
						],
						Projection: {
							ProjectionType: 'ALL'
						}
					};
				}) as LocalSecondaryIndex[];

				const localIndexesDefinitions = _.map(li, index => {
					return {
						AttributeName: index.sort,
						AttributeType: index.type
					};
				}) as AttributeDefinition[];

				// @ts-ignore-next-line
				return this.client.send(
					new CreateTableCommand({
						AttributeDefinitions: _.uniqBy(
							[
								{
									AttributeName: this.schema.sort,
									AttributeType: 'S'
								},
								{
									AttributeName: this.schema.partition,
									AttributeType: 'S'
								},
								...globalIndexesDefinitions,
								...localIndexesDefinitions
							],
							'AttributeName'
						),
						BillingMode: 'PAY_PER_REQUEST',
						KeySchema: [
							{
								AttributeName: this.schema.partition,
								KeyType: 'HASH'
							},
							{
								AttributeName: this.schema.sort,
								KeyType: 'RANGE'
							}
						],
						GlobalSecondaryIndexes: globalIndexes,
						LocalSecondaryIndexes: localIndexes,
						TableName: this.table
					})
				);
			}
		}

		return {} as DescribeTableCommandOutput;
	}
}

export { concatConditionExpression, concatUpdateExpression };
export default Dynamodb;
