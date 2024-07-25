const express = require("express");
const Client = require('pg').Client
const cors = require("cors");

const PORT = process.env.PORT || 3001;
var API = require('groupme').Stateless
awaitAsync = require('express-async-handler')
/**
 * Retrieves user information from the GroupMe API using the provided access token.
 * @param {string} TOKEN - The access token for the GroupMe API.
 * @returns {Promise<Object>} - A promise that resolves to the user object.
 */
function getUserFromAPI(TOKEN) {
	return new Promise((resolve, reject) => {
		API.Users.me(TOKEN, function (err, ret) {
			if (!err) {
				resolve(ret);
			}
			else {
				reject(err);
			}
		});
	});
}

/**
 * Retrieves groups information from the GroupMe API using the provided access token.
 * @param {string} TOKEN - The access token for the GroupMe API.
 * @returns {Promise<Object>} - A promise that resolves to an array of groups object.
 */
function getGroupsFromAPI(TOKEN) {
	return new Promise((resolve, reject) => {
		groups = null
		API.Groups.index(TOKEN, function (err, ret) {
			if (!err) {
				resolve(ret)
			}
			else {
				reject(err)
			}

		});
	});
}

/**
 * Recursively etrieves Messages from the GroupMe API using the provided access token
 * @param {string} TOKEN - The access token for the GroupMe API.
 * @param {int} groupid - The group id to get messages from.
 * @param {Object} opts - The options object for the API call. Limit: (Max 100), before_id: (Message ID to get messages before)
 * @returns {Promise<Object>} - A promise that resolves to an array of messages object.
 */
function recGetMessagesFromAPI(TOKEN, groupid, opts) {
	return new Promise((resolve, reject) => {
		API.Messages.index(TOKEN, groupid, opts, function (err, ret) {
			if (!err) {
				resolve(ret)
			}
			else {
				reject(err)
			}
		});

	});
}

/**
 * Retrieves Messages from the GroupMe API using the provided access token, calls recGetMessagesFromAPI to get all messages.
 * @param {string} TOKEN - User access token for the GroupMe API.
 * @param {int} groupid - The group id to get messages from.
 * @returns {Promise<Object>} - A promise that resolves to an array of messages object.
 */
async function getMessagesFromAPI(TOKEN, gID) {
	let opts = { limit: 100 };
	let messages = [];
	do {
		await recGetMessagesFromAPI(TOKEN, gID, opts).then((x, y) => {
			if (y) {
				batch = [];
			} else {
				batch = x.messages;
			}
		});

		messages = messages.concat(batch);

		if (batch.length < 100) {
			break;
		}

		opts = { limit: 100, before_id: batch[batch.length - 1].id };
	} while (true);

	return messages;
}

/**
 * Replaces all instances of ' with '' in a string, and then wraps the string in single quotes.
 * @param {string} str - String to be sanitized
 * @returns {string} - A sanitized string.
 */
function sanitizeString(str) {
	if (str == null) {
		return "Null";
	}
	return `\'${str.replaceAll('\'', "\'\'")}\'`;
}

/**
 * Replaces all instances of ' with '' in a string, and DOES NOT wrap the string in single quotes.
 * @param {string} str - String to be sanitized
 * @returns {string} - A sanitized string.
 */
function sanitizeNoWrapString(str) {
	if (str == null) {
		return "Null";
	}
	return `${str.replaceAll('\'', "\'\'")}`;
}


/**
 * Gets the number of likes on a message.
 * @param {Object} element Messages to check likes on
 * @returns {int} Number of likes
 */
function getLikes(element) {
	if (!element.reactions) {
		return 0;
	}
	return element.reactions[0].user_ids.length;
}

/**
 * Registers messages in the database if they are not already registered.
 * @param {Object[]} messages - An array of message objects to be added to DB
 * @returns {Promise<void>} - A promise that resolves when the messages are registered.
 */
async function registerMessages(messages) {
	const client = new Client({
		user: 'postgres',
		password: 'password',
		host: 'localhost',
		port: 5432,
		database: 'groupme'
	})

	
	let query = ""
	query += `BEGIN;`
	query += `DELETE FROM messages WHERE conv_id = ${messages[0].group_id};`


	for (let i = 0; i < messages.length; i++) {
		let element = messages[i];
		if (isNaN(element.sender_id)) {
			continue;
		}
		//Aprox 30 messages per second if you send each query here
		query += (`INSERT INTO messages (msg_id, conv_id, sender_id, text, time_sent, num_likes, attachments) VALUES (${element.id}, ${element.group_id}, ${element.sender_id}, ${sanitizeString(element.text)}, ${element.created_at}, ${getLikes(element)}, \'${JSON.stringify(element.attachments)}\');`)

	}
	query += `COMMIT;`
	await client.connect()
	await client.query(query)
	await client.end()
}

/**
 * Registers a user in the Chatters table if they are not already registered.
 * @param {Object} user - The user object to be added to the DB.
 */
async function registerChatters(user) {
	try {
		const client = new Client({
			user: 'postgres',
			password: 'password',
			host: 'localhost',
			port: 5432,
			database: 'groupme'
		})

		await client.connect()

		const isReg = await client.query(`SELECT * FROM chatters WHERE id = \'${user.id}\';`)
		if (isReg.rowCount == 0) {
			await client.query(`INSERT INTO chatters (name, id) VALUES (\'${user.name}\', ${user.id});`)
		}else {
			await client.end();
		}

	} catch (error) {
		console.log(error)
	}
}
/**
 * Marks a group as loaded in the database.
 * @param {Object} group 
 */
async function markGroupLoaded(group) {
	try {
		const client = new Client({
			user: 'postgres',
			password: 'password',
			host: 'localhost',
			port: 5432,
			database: 'groupme'
		})

		await client.connect()
		const isReg = await client.query(`
		SELECT * FROM conversations 
		WHERE conv_id = ${group} and loaded = true;`)
		if (isReg.rowCount == 0) {
			await client.query(`
			UPDATE conversations 
			SET loaded = true 
			WHERE conv_id = ${group};`)
		}
		await client.end();

	} catch (error) {
		console.log(error)
	}
}
/**
 * Adds Group information to conversations table if it is not already registered.
 * @param {groups} groups 
 */
async function registerConversations(groups) {
	try {
		const client = new Client({
			user: 'postgres',
			password: 'password',
			host: 'localhost',
			port: 5432,
			database: 'groupme'
		})

		await client.connect()
		for (let i = 0; i < groups.length; i++) {
			let group = groups[i]
			const isReg = await client.query(`SELECT * FROM conversations WHERE conv_id = ${group.id};`)

			if (isReg.rowCount == 0) {
				await client.query(`
				INSERT INTO conversations (conv_id, name, msg_count, loaded) 
				VALUES (${group.id}, ${sanitizeString(group.name)}, ${group.messages.count}, false);`)
			}
		}
		await client.end();

	} catch (error) {
		console.log(error)
	}
}

async function pruneGroupsToLoad(groupids) {
	let query = `SELECT c.conv_id FROM conversations c WHERE c.loaded = true;`;
	const client = new Client({
		user: 'postgres',
		password: 'password',
		host: 'localhost',
		port: 5432,
		database: 'groupme'
	});
	await client.connect();
	const response = await client.query(query);
	await client.end();

	//if a groupid appears in response remove it from the list
	for (let i = 0; i < response.rows.length; i++) {
		let group = response.rows[i];
		let index = groupids.indexOf(group.conv_id.toString());
		if (index > -1) {
			groupids.splice(index, 1);
		}
	}

	return groupids;
}

/**
 * Registers user groups in the database if they are not already registered.
 * @param {number} id - The user id.
 * @param {Object[]} groups - An array of group objects containing group information.
 * @returns {Promise<void>} - A promise that resolves when the user groups are registered.
 */
async function registerMemberOf(groups) {
	try {
		const client = new Client({
			user: 'postgres',
			password: 'password',
			host: 'localhost',
			port: 5432,
			database: 'groupme'
		})


		let query = ""
		await client.connect()
		for (let i = 0; i < groups.length; i++) {
			let group = groups[i]
			for (let j = 0; j < group.members_count; j++) {
				let member = group.members[j]
				await client.query(`SELECT 1 FROM member WHERE userid = ${parseInt(member.user_id)} and groupid = ${parseInt(group.id)};`).then(async (x) => {
					if (x.rowCount == 0) {
						console.log("Added Group " + group.name + " to user " + member.nickname)
						try {
							await client.query(`INSERT INTO member (userid, memid, groupid, nickname) VALUES (${member.user_id}, ${member.id} ,${group.id}, ${sanitizeString(member.nickname)});`)
						} catch (error) {
							console.log(error)
						}
					} else {
						console.log("Group " + group.name + " already added to user " + member.nickname)
					}
				})
			}
		}
		await client.end()
	} catch (error) {
		console.log(error)
	}
}

const app = express();
app.use(cors());

/**
 * Endpoint for user login.
 * @name GET /api/login
 * @function
 * @async
 * @param {Object} req - The request object.
 * @param {Object} res - The response object.
 */
app.get("/api/login", awaitAsync(async (req, res) => {
	const TOKEN = req.query["access_token"];

	let user = await getUserFromAPI(TOKEN);
	let convs = await getGroupsFromAPI(TOKEN)
	await registerChatters(user)
	await registerConversations(convs)
	await registerMemberOf(convs)

	//Return 200
	res.status(200).send("Success")
}));

app.get("/api/loadmessages", awaitAsync(async (req, res) => {
	const TOKEN = req.query["access_token"];
	let groupids = req.query["groupids"].split(",");
	let messages = []

	groupids = await pruneGroupsToLoad(groupids);

	for (let i = 0; i < groupids.length; i++) {
		const group = await groupids[i]
		
		console.log("Loading messages from group " + group + "...")
		messages = await getMessagesFromAPI(TOKEN, group);

		console.log("Registering Messages")
		await registerMessages(messages)
		markGroupLoaded(group)
	}

	console.log("Done")
	res.status(200).send(`Added ${messages.length} to DB`)
}));

app.get("/api/getgroups", awaitAsync(async (req, res) => {
	const TOKEN = req.query["access_token"];
	let rawGroups = await getGroupsFromAPI(TOKEN)
	let groups = []
	rawGroups.forEach(group => {
		groups.push({ id: group.id, name: group.name })
	});

	res.status(200).send(groups)
}));

app.get("/api/getloadedgroups", awaitAsync(async (req, res) => {
	const TOKEN = req.query["access_token"];
	const user = await getUserFromAPI(TOKEN);
	const userID = user.id;
	const query = `
	SELECT * FROM conversations c 
	JOIN member m ON c.conv_id = m.groupid 
	WHERE c.loaded = true and m.userid = ${parseInt(userID)};`
	
	const client = new Client({
		user: 'postgres',
		password: 'password',
		host: 'localhost',
		port: 5432,
		database: 'groupme'
	})
	await client.connect()
	const response = await client.query(query)
	await client.end()
	res.status(200).send(response.rows)

}));

app.get("/api/avgLikes", awaitAsync(async (req, res) => {
	const groupID = req.query["groupID"];
	const query = `
		SELECT m2.nickname as name, AVG(m.num_likes) as value
		FROM messages m
		JOIN member m2 ON m.sender_id = m2.userid and m.conv_id = m2.groupid 
		WHERE m.conv_id = ${parseInt(groupID)}
		group by m2.nickname
		order by AVG(m.num_likes) desc;`;

	const client = new Client({
		user: 'postgres',
		password: 'password',
		host: 'localhost',
		port: 5432,
		database: 'groupme'
	})
	await client.connect()
	const response = await client.query(query)
	await client.end()
	res.status(200).send(response.rows)
}));

app.get("/api/totalMessages", awaitAsync(async (req, res) => {
	const groupID = req.query["groupID"];
	const query = `
	SELECT m2.nickname as name, COUNT(*) as value
	FROM messages m
	JOIN member m2 ON m.sender_id = m2.userid and m.conv_id = m2.groupid 
	WHERE m.conv_id = ${parseInt(groupID)}
	group by m2.nickname
	order by COUNT(*) desc;`;

	const client = new Client({
		user: 'postgres',
		password: 'password',
		host: 'localhost',
		port: 5432,
		database: 'groupme'
	})
	await client.connect()
	const response = await client.query(query)
	await client.end()
	res.status(200).send(response.rows)
}));

app.get("/api/totalLikes", awaitAsync(async (req, res) => {
	const groupID = req.query["groupID"];
	const query = `
	SELECT m2.nickname as name, SUM(m.num_likes) as value
		FROM messages m
		JOIN member m2 ON m.sender_id = m2.userid and m.conv_id = m2.groupid 
		WHERE m.conv_id = ${parseInt(groupID)}
		group by m2.nickname
		order by SUM(m.num_likes) desc;`;

	const client = new Client({
		user: 'postgres',
		password: 'password',
		host: 'localhost',
		port: 5432,
		database: 'groupme'
	})
	await client.connect()
	const response = await client.query(query)
	await client.end()
	res.status(200).send(response.rows)
}));

app.get("/api/mostLiked", awaitAsync(async (req, res) => {
	const groupID = req.query["groupID"];
	const query = `
	SELECT m2.nickname as name, m.num_likes as likes, m."text" as msg, m.attachments as atch
		FROM messages m
		JOIN member m2 ON m.sender_id = m2.userid and m.conv_id = m2.groupid 
		WHERE m.conv_id = ${parseInt(groupID)}
		and m.num_likes = (SELECT MAX(m.num_likes) FROM messages m where m.conv_id = ${parseInt(groupID)})`;

	const client = new Client({
		user: 'postgres',
		password: 'password',
		host: 'localhost',
		port: 5432,
		database: 'groupme'
	})
	await client.connect()
	const response = await client.query(query)
	await client.end()
	res.status(200).send(response.rows)
}));

app.get("/api/random", awaitAsync(async (req, res) => {
	const groupID = req.query["groupID"];
	const query = `
	SELECT m2.nickname as name, m.num_likes as likes, m."text" as msg, m.attachments as atch
		FROM messages m
		JOIN member m2 ON m.sender_id = m2.userid and m.conv_id = m2.groupid 
		WHERE m.conv_id = ${parseInt(groupID)}
		ORDER BY random()
		LIMIT 1;`;

	const client = new Client({
		user: 'postgres',
		password: 'password',
		host: 'localhost',
		port: 5432,
		database: 'groupme'
	})
	await client.connect()
	const response = await client.query(query)
	await client.end()
	res.status(200).send(response.rows)
}));

app.get("/api/custom", awaitAsync(async (req, res) => {
	const groupID = parseInt(req.query["groupID"]);
	const likes = parseInt(req.query["likes"]) || 0;
	const likesValue = parseInt(req.query["likesValue"]) || 0;
	const fromUser = parseInt(req.query["fromUser"]) || 0;
	const fromUserValue = req.query['fromUserValue'] || "";
	let likesQ = ""
	let fromUserQ = ""
	let likesSign = ""
	if (likes > 0) {
		switch (likes) {
			case 1:
				likesSign = ">"
				break;
			case 2:
				likesSign = "<"
				break;
			case 3:
				likesSign = "="
				break;
			case 4:
				likesSign = ">="
				break;
			case 5:
				likesSign = "<="
				break;
		}
		likesQ = `and m.num_likes ${likesSign} ${likesValue}`
	}
	if (fromUser > 0) {
		fromUserQ = `and m2.nickname like \'%${sanitizeNoWrapString(fromUserValue)}%\'`
	}

	const query = `SELECT m2.nickname as name, m.num_likes as likes, m.text as msg, m.attachments as atch FROM messages m JOIN member m2 ON m.sender_id = m2.userid and m.conv_id = m2.groupid WHERE m.conv_id = ${parseInt(groupID)} ${likesQ} ${fromUserQ} LIMIT 20;`;

	const client = new Client({
		user: 'postgres',
		password: 'password',
		host: 'localhost',
		port: 5432,
		database: 'groupme'
	})
	await client.connect()
	const response = await client.query(query)
	await client.end()
	console.log(response.rows)
	res.status(200).send(response.rows)
}));


app.listen(PORT, () => {
	console.log(`Server listening on ${PORT}`);
});



