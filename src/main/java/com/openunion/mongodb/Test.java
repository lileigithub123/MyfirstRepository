package com.openunion.mongodb;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.addEachToSet;
import static java.util.Arrays.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import com.google.common.collect.Lists;
import com.mongodb.Block;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.result.UpdateResult;

public class Test {
	public static String HOST = "127.0.0.1";
	public static String PORT = "27017";

	public static void main(String[] args) {
		try {
			CodecRegistry codecRegistry = CodecRegistries
					.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
					CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));

			// MongoClientSettings settings =
			// MongoClientSettings.builder().codecRegistry(codecRegistry).build();
			// 通过连接认证获取MongoDB连接
			String user = "admin"; // the user name
			String source = "admin"; // the source where the user is defined
			char[] password = "123456".toCharArray(); // the password as a character array
			MongoCredential credential = MongoCredential.createCredential(user, source, password);
			MongoClient mongoClient = MongoClients.create(MongoClientSettings.builder().codecRegistry(codecRegistry)
					.applyToClusterSettings(
							builder -> builder.hosts(asList(new ServerAddress("10.1.230.39", 27017))))
					.credential(credential).readConcern(ReadConcern.MAJORITY).writeConcern(WriteConcern.MAJORITY)
					.build());
			// MongoClient mongoClient = MongoClients.create("mongodb://10.1.230.39:27017");

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("test");
			System.out.println("MongoDBConnect to database successfully");

			// 创建集合
			// mongoDatabase.createCollection("test");
			// System.out.println("集合创建成功");
			// 选择集合
			MongoCollection<Document> collection = mongoDatabase.getCollection("users")
					.withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.MAJORITY)
					.withWriteConcern(WriteConcern.MAJORITY);
			;

			/**
			 * 插入文档 1. 创建文档 org.bson.Document 参数为key-value的格式 2. 创建文档集合List<Document> 3.
			 * 将文档集合插入数据库集合中 mongoCollection.insertMany(List<Document>) 插入单个文档可以用
			 * mongoCollection.insertOne(Document)
			 */
			// Document document = new Document("title", "MongoDB3").append("description",
			// "database").append("likes", 100)
			// .append("by", "Fly");
			// List<Document> documents = new ArrayList<Document>();
			// documents.add(document);
			// collection.insertMany(documents);
			// System.out.println("文档插入成功");

			final List<Document> ret = new ArrayList<>();
			Consumer<Document> printBlock = new Consumer<Document>() {
				@Override
				public void accept(Document t) {
					System.out.println(t.toJson());
					ret.add(t);
				}
			};
			// select * from users where favorites.cites has "东莞"、"东京"
			FindIterable<User> find = collection.find(all("favorites.cites", asList("东莞", "上海")), User.class);
			List<User> userList = new ArrayList<>();
			find.forEach(new Consumer<User>() {
				@Override
				public void accept(User u) {
					System.out.println(u.getUsername() + "id=" + u.getId());
					userList.add(u);
				}
			});
			// MongoIterable<TResult>
			// Function<T, R>
			MongoIterable<User> l = find.map(a -> a);// 函数式编程jdk1.8的用法， 箭头后边为Function类的apply方法的实现，箭头前为apply方法的参数
			l.forEach(new Consumer<User>() {
				@Override
				public void accept(User t) {
					System.out.println(t.getUsername());
					// ret.add(t);
				}
			});
			Iterator<User> q = l.iterator();
			List<User> uList = Lists.newArrayList(q);// google的第三方类库 Iterator转List
			System.out.println(uList.size());
			while (q.hasNext()) {
				User d = q.next();
				System.out.println(d);
			}
			// FindIterable<Document> find2 = collection.find(all("favorites.cites",
			// Arrays.asList("东莞", "上海")));
			List<User> uuList = new ArrayList<>();
			find.into(uuList);
			uuList.forEach(u -> {
				System.out.println("uuuuuuuuuuuuuuuuuuuu=" + u.getUsername());
			});
			// System.out.println(String.valueOf(ret.size()));
			ret.removeAll(ret);

			Block<ChangeStreamDocument<Document>> printBlock2 = new Block<ChangeStreamDocument<Document>>() {
				@Override
				public void apply(final ChangeStreamDocument<Document> changeStreamDocument) {
					System.out.println(changeStreamDocument);
				}
			};

			// List<ChangeStreamDocument<Document>> changeList = new ArrayList<>();
			// List<Document> changeDocumentList = new ArrayList<>();
			// collection
			// .watch(asList(
			// Aggregates.match(in("operationType", asList("insert", "update", "replace",
			// "delete")))))
			// .fullDocument(FullDocument.UPDATE_LOOKUP).into(changeList);
			// changeList.forEach(w -> {
			// Document d = w.getFullDocument();
			// changeDocumentList.add(d);
			// System.out.println(d.toJson());
			// });


			// /**
			// * 检索所有文档 1. 获取迭代器FindIterable<Document> 2. 获取游标MongoCursor<Document> 3.
			// * 通过游标遍历检索出的文档集合
			// */
			// FindIterable<Document> findIterable = collection.find();
			// MongoCursor<Document> mongoCursor = findIterable.iterator();
			// while (mongoCursor.hasNext()) {
			// System.out.println(mongoCursor.next());
			// }
			// System.out.println("检索所有文档成功");
			//
			// // 更新文档 将文档中likes=100的文档修改为likes=200
			// collection.updateMany(Filters.eq("likes", 100), new Document("$set", new
			// Document("likes", 200)));
			// // 检索查看结果
			// findIterable = collection.find();
			// mongoCursor = findIterable.iterator();
			// while (mongoCursor.hasNext()) {
			// System.out.println(mongoCursor.next());
			// }
			// System.out.println("更新文档成功");
			//
			// // 删除符合条件的第一个文档
			// collection.deleteOne(Filters.eq("likes", 200));
			// // 删除所有符合条件的文档
			// collection.deleteMany(Filters.eq("likes", 200));
			// // 检索查看结果
			// findIterable = collection.find();
			// mongoCursor = findIterable.iterator();
			// while (mongoCursor.hasNext()) {
			// System.out.println(mongoCursor.next());
			// }
			// System.out.println("删除文档成功");

//			Document document = new Document();
//			document.append("userName", "张三");
//			document.append("password", "12345678");
//
//			Map<String, String> addressMap = new HashMap<>();
//			addressMap.put("code", "100102");
//			addressMap.put("add", "北京市朝阳区");
//
//			Document subDocument1 = new Document();
//			subDocument1.append("code", "100102");
//			subDocument1.append("add", "北京市朝阳区");
//			document.append("address", subDocument1);
//
//			Map<String, Object> favoriteMap = new HashMap<>();
//			favoriteMap.put("movies", Arrays.asList("疯狂外星人", "地球之谜"));
//			favoriteMap.put("cites", Arrays.asList("北京", "唐山", "石家庄"));
//			Document subDocument2 = new Document();
//			subDocument2.append("movies", Arrays.asList("疯狂外星人", "地球之谜"));
//			subDocument2.append("cites", Arrays.asList("北京", "唐山", "石家庄"));
//
//			document.append("favorites", subDocument2);
//
//			collection.insertMany(Arrays.asList(document));
//
//			Document document2 = new Document("name", "Café Con Leche")
//					.append("contact",
//							new Document("phone", "228-555-0149").append("email", "cafeconleche@example.com")
//									.append("location", Arrays.asList(-73.92502, 40.8279556)))
//					.append("stars", 3).append("categories", Arrays.asList("Bakery", "Coffee", "Pastries"));
//			collection.insertOne(document2);
//			
//			collection.updateOne(eq("username","lison"), new Document("$set",new Document("age","6")));
//			
//			UpdateResult updateMany2 = collection.updateMany(eq("favorites.cites", "东莞"), 
//                    addEachToSet("favorites.movies", Arrays.asList( "小电影2 ", "小电影3")));
//			System.out.println(updateMany2.getModifiedCount());
			
			//select * from users  where username like '%s%' and (contry= English or contry = USA)
			String regexStr = ".*s.*";
			Bson reg = regex("username", regexStr);
			Bson or = or(eq("country","English"),eq("contry","USA"));
			FindIterable<Document> it = collection.find(and(reg,or));
			List<Document> dcoList = new ArrayList<>();
			it.forEach(printBlock);
			it.into(dcoList);
			System.out.println("ret size="+dcoList.size());
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}

	}

	public void insert() {
		Document document = new Document();
		document.append("userName", "张三");
		document.append("password", "12345678");

		Map<String, String> addressMap = new HashMap<>();
		addressMap.put("code", "100102");
		addressMap.put("add", "北京市朝阳区");
		document.put("address", addressMap);

		Map<String, Object> favoriteMap = new HashMap<>();
		favoriteMap.put("movies", Arrays.asList("疯狂外星人", "地球之谜"));
		favoriteMap.put("cites", Arrays.asList("北京", "唐山", "石家庄"));
		document.put("favorites", favoriteMap);
	}

}
