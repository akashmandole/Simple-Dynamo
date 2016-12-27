package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	String myPort;

	static final String database_name = "dynamo_db";
	static final String table_name = "dynamo_record";

	static final String create_table = "CREATE TABLE "+table_name+" (key TEXT, value TEXT);";
	static final int SERVER_PORT = 10000;

	static boolean lock = false;
	static String orignalQueryPort = null;
	public static HashMap<String, String> cursorList = new HashMap<String, String>();

	public static HashMap<String, Boolean> quorumReplicationQuery = new HashMap<String, Boolean>();
	public static HashMap<String, Boolean> quorumReplicationInsert = new HashMap<String, Boolean>();

	public static boolean quorumReplication = false;

	static volatile Object queryLock = new Object();
	static volatile Object updateLock = new Object();


	static public void acquireLock() {
		lock = true;
	}

	static public void releaseLock() { lock = false; }


	private static class DBHelper extends SQLiteOpenHelper {
		DBHelper(Context context){
			super(context, database_name, null, 1);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL(create_table);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			db.execSQL("DROP TABLE IF EXISTS " +  table_name + ";");
			onCreate(db);
		}
	}

	private DBHelper dbHelper;


	@Override
	public String getType(Uri uri) {
		return null;
	}

	// compare two strings
	private boolean compare(String s1, String s2) {
		if(s1.compareTo(s2)>0) {
			return true;
		} else {
			return false;
		}
	}

	public class ClientTask extends AsyncTask<String, Void, Void> {

		Message message;

		public ClientTask(Message message) {
			this.message = message;
		}

		@Override
		protected Void doInBackground(String... msgs) {
			try {

				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0]));
				socket.setSoTimeout(100);

				ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
				objectOutputStream.writeObject(message);
				objectOutputStream.flush();
				objectOutputStream.close();
				socket.close();

			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			}

			return null;
		}
	}


	@Override
	public Uri insert(Uri uri, ContentValues values)
	{
		try {

			String key = (String) values.get("key");
			String keyHash = genHash(key);

			synchronized (updateLock) {

				String portToInsert = myPort;

				do {

					String myHash = genPortHash(portToInsert);
					String predecessorPort = ServerTask.getPredecessorPort(portToInsert);
					String predecessorHash = genPortHash(predecessorPort);

					boolean case1 = myHash.equals(predecessorHash);
					boolean case2 = (compare(myHash, keyHash) && compare(keyHash, predecessorHash));
					boolean case3 = (compare(predecessorHash, myHash) && compare(myHash, keyHash));
					boolean case4 = (compare(predecessorHash, myHash) && compare(keyHash, predecessorHash));

					if (case1||case2||case3||case4) {
						// found the port to insert break now
						break;
					} else {
						portToInsert = ServerTask.getSuccessorPort(portToInsert);
					}
				} while(true);

				boolean case1 = portToInsert.equals(myPort); // inserting first time
				boolean case2 = quorumReplicationInsert.containsKey(key); // replication

				if( case1 || case2) {

					SQLiteDatabase db_write = dbHelper.getWritableDatabase();
					long id = db_write.insertWithOnConflict(table_name, null, values, SQLiteDatabase.CONFLICT_REPLACE);

					if (quorumReplicationInsert.containsKey(key)) {
						quorumReplicationInsert.remove(key);
					}
					else {
						// replicate to successors
						Message forwardToSuccessor = new Message("insert",(String) values.get("key"),(String) values.get("value"));

						String succ1 = ServerTask.getSuccessorPort(portToInsert);
						String succ2 = ServerTask.getSuccessorPort(succ1);

						// succ1
						new ClientTask(forwardToSuccessor).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, succ1);
						// succ2
						new ClientTask(forwardToSuccessor).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, succ2);

					}

					if (id > 0) {
						uri = Uri.withAppendedPath(uri, Long.toString(id));
						return uri;
					}

				} else {

					// send message to port and two successors
					Message forwardToSuccessor = new Message("insert",(String) values.get("key"),(String) values.get("value"));

					String succ1 = ServerTask.getSuccessorPort(portToInsert);
					String succ2 = ServerTask.getSuccessorPort(succ1);

					//port to insert
					new ClientTask(forwardToSuccessor).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portToInsert);

					// succ1
					new ClientTask(forwardToSuccessor).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, succ1);
					// succ2
					new ClientTask(forwardToSuccessor).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, succ2);

				}

				quorumReplicationInsert.remove(key);
				return null;
			}
		} catch (NoSuchAlgorithmException e){
			e.printStackTrace();
		}
		return null;
	}


	public static String genPortHash(String port) throws NoSuchAlgorithmException {
		return genHash(String.valueOf(Integer.parseInt(port) / 2));
	}

	public static String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}


	@Override
	public boolean onCreate() {

		dbHelper = new DBHelper(getContext());

		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			String myHash = genPortHash(myPort);
			new ServerTask(getContext(), myPort, myHash).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		return true;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {

		SQLiteDatabase db_read = dbHelper.getReadableDatabase();

		Cursor cursor;

		try {
			if (selection.equals("@")) {
				cursor = db_read.rawQuery("Select * from " + table_name, selectionArgs);
				Log.v("query completed for : ", selection);
				return cursor;
			} else if (selection.equals("*")) {

				String myHash = genPortHash(ServerTask.myPort);
				String predecessorHash = genPortHash(ServerTask.predecessor);

				if (myHash.equals(predecessorHash)) {
					cursor = db_read.rawQuery("Select * from " + table_name, selectionArgs);
					Log.v("query completed for : ", selection);
					return cursor;

				} else {

					if (orignalQueryPort == null) {
						orignalQueryPort = myPort;
						acquireLock();
					}

					cursor = db_read.rawQuery("Select * from " + table_name, selectionArgs);
					cursor.moveToFirst();
					for (int i = 0; i < cursor.getCount(); i++) {
						cursorList.put(cursor.getString(cursor.getColumnIndex("key")), cursor.getString(cursor.getColumnIndex("value")));
						cursor.moveToNext();
					}
					cursor.close();

					// forward to successor
					Message forwardToSuccessor = new Message(orignalQueryPort,"query_global",selection,cursorList);
					new ClientTask(forwardToSuccessor).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, ServerTask.successorPort1);
					ServerTask.matrixCursor = null;

					while (lock) {
						// wait for some time
					}
					releaseLock();
					orignalQueryPort = null;
					return ServerTask.matrixCursor;
				}

			} else {
				String portToInsert = myPort;

				String key = selection;
				String keyHash = genHash(key);

				do {

					String myHash = genPortHash(portToInsert);
					String predecessorPort = ServerTask.getPredecessorPort(portToInsert);
					String predecessorHash = genPortHash(predecessorPort);

					boolean case1 = myHash.equals(predecessorHash);
					boolean case2 = (compare(myHash, keyHash) && compare(keyHash, predecessorHash));
					boolean case3 = (compare(predecessorHash, myHash) && compare(myHash, keyHash));
					boolean case4 = (compare(predecessorHash, myHash) && compare(keyHash, predecessorHash));

					if (case1||case2||case3||case4) {
						// found the port to query break now
						break;
					} else {
						portToInsert = ServerTask.getSuccessorPort(portToInsert);
					}
				} while(true);

				boolean case1 = portToInsert.equals(myPort); // inserting first time

				if (case1) {
					cursor = db_read.query(table_name, projection, "key=?", new String[]{selection}, null, null, sortOrder);
					Log.v("query completed for : ", selection);
					return cursor;
				}
				else // forward request to the port to query
				{
					synchronized (queryLock) {

						ServerTask.matrixCursor = null;

						if (quorumReplicationQuery.containsKey(selection)) {
							// request served
						} else {
							quorumReplicationQuery.put(selection, true);
						}

						// forward request to mappedPort
						Message forwardToPortToInsert = new Message(myPort,"query",selection,null);
						new ClientTask(forwardToPortToInsert).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portToInsert);

						do {
							if(quorumReplicationQuery.containsKey(selection)) {
								Thread.sleep(100);
							} else {
								break;
							}

						} while(true);
						return ServerTask.matrixCursor;

					}// end synchronized
				}
			}
		}catch(NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs)
	{
		int rows = 0;
		SQLiteDatabase db_write = dbHelper.getWritableDatabase();
		{
			if (selection.equals("@")) {
				rows = db_write.delete(table_name, null, null);

			} else if (selection.equals("*"))
			{
				if (myPort.equals(ServerTask.predecessor)) {
					rows = db_write.delete(table_name, null, null);
					return rows;
				}

				else {

					rows = db_write.delete(table_name, null, null);

					Message m = new Message();
					m.setType("delete_global");
					m.setQuery(selection);
					new ClientTask(m).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, ServerTask.successorPort1);
					return rows;
				}

			} else {
				try {

					String keyHash = genHash(selection);

					String portToInsert = myPort;

					do {

						String myHash = genPortHash(portToInsert);
						String predecessorPort = ServerTask.getPredecessorPort(portToInsert);
						String predecessorHash = genPortHash(predecessorPort);

						boolean case1 = myHash.equals(predecessorHash);
						boolean case2 = (compare(myHash, keyHash) && compare(keyHash, predecessorHash));
						boolean case3 = (compare(predecessorHash, myHash) && compare(myHash, keyHash));
						boolean case4 = (compare(predecessorHash, myHash) && compare(keyHash, predecessorHash));

						if (case1 || case2 || case3 || case4) {
							// found the port to insert break now
							break;
						} else {
							portToInsert = ServerTask.getSuccessorPort(portToInsert);
						}
					} while (true);

					boolean case1 = portToInsert.equals(myPort);
					boolean case2 = quorumReplication;

					if (case1 || case2) {
						rows = db_write.delete(table_name, "key= ?", new String[]{selection});

						if (quorumReplication) {
							quorumReplication = false;
							return rows;
						}
					} else
					{
						Message forwardToPortToDelete = new Message(myPort,"delete",selection,null);
						new ClientTask(forwardToPortToDelete).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portToInsert);
					}

					if (!quorumReplication) {

						String succ1 = ServerTask.getSuccessorPort(portToInsert);
						String succ2 = ServerTask.getSuccessorPort(succ1);

						Message forwardToSucc1 = new Message(myPort,"delete",selection,null);
						new ClientTask(forwardToSucc1).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, succ1);

						Message forwardToSucc2 = new Message(myPort,"delete",selection,null);
						new ClientTask(forwardToSucc2).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, succ2);
					}
					quorumReplication = false;

				}catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}
		}
		return rows;
	}

	public static class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		static final String TAG = ServerTask.class.getSimpleName();

		private Context context;

		public static String myPort;
		public static String myPortHash;

		public static String successorPort1;
		public static String successorPort2;
		public static String predecessor;

		static volatile Object queryLock = new Object();
		static volatile Object updateLock = new Object();

		public static Cursor matrixCursor = null;

		final String url = "content://edu.buffalo.cse.cse486586.simpledynamo.provider";
		final Uri simple_dynamo_uri = Uri.parse(url);


		public ServerTask(Context context, String myPort, String myPortHash) throws NoSuchAlgorithmException {
			this.context = context;
			this.myPort = myPort;
			this.myPortHash = myPortHash;
			this.successorPort1 = getSuccessorPort(this.myPort);
			this.successorPort2 = getSuccessorPort(this.successorPort1);
			this.predecessor = getPredecessorPort(this.myPort);

		}

		public static String getSuccessorPort(String myPort) {
			if (myPort.equals("11124")) {
				return "11112";
			} else if (myPort.equals("11112")) {
				return "11108";
			} else if (myPort.equals("11108")) {
				return "11116";
			} else if (myPort.equals("11116")) {
				return "11120";
			} else if (myPort.equals("11120")) {
				return "11124";
			} else {
				return myPort;
			}
		}

		public static String getPredecessorPort(String myPort) {
			if (myPort.equals("11112")) {
				return "11124";
			} else if (myPort.equals("11108")) {
				return "11112";
			} else if (myPort.equals("11116")) {
				return "11108";
			} else if (myPort.equals("11120")) {
				return "11116";
			} else if (myPort.equals("11124")) {
				return "11120";
			} else {
				return myPort;
			}
		}
		private void forwardRequest(Message message,String port) {
			try {

				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));

				ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
				objectOutputStream.writeObject(message);
				objectOutputStream.flush();
				objectOutputStream.close();
				socket.close();

			} catch (UnknownHostException e) {
				Log.e(TAG, "Unable to forward request");
			} catch (IOException e) {
				Log.e(TAG, "Unable to forward request");
			}
		}
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			while(true) {
				try {

					Socket serverListener = serverSocket.accept();
					ObjectInputStream objectInputStream = new ObjectInputStream(serverListener.getInputStream());
					Message message = (Message) objectInputStream.readObject();


					if (message.type.equals("insert")) {
						synchronized (updateLock) {
							SimpleDynamoProvider.quorumReplicationInsert.put(message.getKey(), true);

							ContentValues keyValueToInsert = new ContentValues();
							keyValueToInsert.put("key", message.getKey());
							keyValueToInsert.put("value", message.getValue());
							context.getContentResolver().insert(simple_dynamo_uri, keyValueToInsert);
						}
					} else if (message.type.equals("query")) {
						synchronized (queryLock) {
							Cursor cursor = context.getContentResolver().query(simple_dynamo_uri, null, message.getQuery(), null, null);
							cursor.moveToFirst();

							Message queryAck = new Message(message.getMyPort(),"query_ack",message.getQuery(),cursor.getString(cursor.getColumnIndex("key")),cursor.getString(cursor.getColumnIndex("value")));
							forwardRequest(queryAck,message.getMyPort());

						}
					} else if (message.type.equals("query_ack")) {
						synchronized (queryLock) {
							matrixCursor = new MatrixCursor(new String[]{"key", "value"});

							((MatrixCursor) matrixCursor).addRow(new String[]{
									message.getKey(), message.getValue()});

							SimpleDynamoProvider.quorumReplicationQuery.remove(message.getQuery());
						}
					} else if (message.type.equals("query_global")) {
						synchronized (queryLock) {
							matrixCursor = null;
							SimpleDynamoProvider.orignalQueryPort = message.getMyPort();
							SimpleDynamoProvider.releaseLock();

							if (SimpleDynamoProvider.orignalQueryPort.equals(myPort)) {
								matrixCursor = new MatrixCursor(new String[]{"key", "value"});

								Iterator iterator = message.getCursorList().entrySet().iterator();

								while (iterator.hasNext()) {

									Map.Entry entry = (Map.Entry) iterator.next();
									((MatrixCursor) matrixCursor).addRow(new String[]{(String) entry.getKey(), (String) entry.getValue()});

								}

								SimpleDynamoProvider.releaseLock();

							} else {
								SimpleDynamoProvider.cursorList = (HashMap<String, String>) message.getCursorList();
								context.getContentResolver().query(simple_dynamo_uri, null, message.getQuery(), null, null);

							}

							SimpleDynamoProvider.releaseLock();
							SimpleDynamoProvider.orignalQueryPort = null;
						}
					} else if (message.type.equals("delete")) {
						synchronized (updateLock) {

							SimpleDynamoProvider.quorumReplication = true;
							context.getContentResolver().delete(simple_dynamo_uri, message.getQuery(), null);
							SimpleDynamoProvider.quorumReplication = false;
						}
					} else if (message.type.equals("delete_global")) {
						synchronized (updateLock) {

							SimpleDynamoProvider.quorumReplication = true;
							int rows = context.getContentResolver().delete(simple_dynamo_uri, message.getQuery(), null);
							SimpleDynamoProvider.quorumReplication = false;
						}
					}

					objectInputStream.close();
					serverListener.close();

				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
	}

}