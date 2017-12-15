package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String [] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
	static final String [] REMOTE_ID = {"5554", "5556", "5558", "5560", "5562"};
	private static String [] KEY_RING;
	static final int SERVER_PORT = 10000;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private static final int AVD_NO = 5;
	private static String myPort;
	private static String myID;
	private Integer myClock;
	private static final int TIMEOUT = 500;

	private String mID;
	private String mSuccessor;
	private String mPredecessor;

	private HashMap<String, String> portTable;
	private HashMap<String, Integer> indexTable;

	private static Integer N = 3;
	private static Integer W = 2;
	private static Integer R = 2;

	private class VectorClock {
		public HashMap<String, String> clocks;
		public VectorClock() {
			clocks = new HashMap<String, String>();
		}
		public void updateClock(String node, String clock) {
			if (clocks.containsKey(node)) {
				clocks.remove(node);
			}
			clocks.put(node, clock);
		}
	}

	//private class ObjectSignature {
	//	public String key;
	//	//String val;
	//	public VectorClock vc;
	//	public ObjectSignature() {
	//		key = "";
	//		//val = "";
	//		vc = new VectorClock();
	//	}
	//}

	private class ObjectList {
		public HashMap<String, VectorClock> objects;
		public ObjectList() {
			objects = new HashMap<String, VectorClock>();
		}
		public void writeObject(String key, String val, String node, String clock) {
			if (objects.containsKey(key)) {
				VectorClock vc = objects.get(key);
				vc.updateClock(node, clock);
				objects.remove(key);
				objects.put(key, vc);
			}
			else {
				VectorClock vc = new VectorClock();
				vc.updateClock(node, clock);
				objects.put(key, vc);
			}
			File file = new File(getContext().getFilesDir().getAbsolutePath(), key);
			if (file.exists() && file.isFile()) file.delete();
			try {
				FileWriter fWrite = new FileWriter(file);
				fWrite.write(val);
				fWrite.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			Log.v("WriteObject", key);
		}

		public ObjectVersioning readObject(String key) {
			if (!objects.containsKey(key)) {
				Log.v("ReadObject", "Read null: " + key);
				return null;
			}

			ObjectVersioning obv = new ObjectVersioning();
			obv.key = key;
			obv.val = "";
			obv.vc = objects.get(key);

			File file = new File(getContext().getFilesDir().getAbsolutePath(), key);
			if (file.exists() && file.isFile()) {
				try {
					FileReader fReader = new FileReader(file);
					BufferedReader bReader = new BufferedReader(fReader);
					obv.val = bReader.readLine();
					bReader.close();
					fReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			Log.v("ReadObject", key);
			return obv;
		}

		public boolean isContained(String key) {
			return objects.containsKey(key);
		}

		public void deleteObject(String key) {
			objects.remove(key);
			File file = new File(getContext().getFilesDir().getAbsolutePath(), key);
			if (file.exists() && file.isFile()) file.delete();
		}
	}
	private ObjectList obList;

	private class ObjectVersioning {
		public String key;
		public String val;
		public VectorClock vc;
		public ObjectVersioning() {
			vc = new VectorClock();
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		if (selection==null) return 0;
		String key = selection;

		String num = "0";
		if (key.equals("*")) {
			try {
				Socket socket = new Socket();
				socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(myPort)), TIMEOUT);
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());
				DataInputStream in = new DataInputStream(socket.getInputStream());
				out.writeUTF("deleteAll");
				num = in.readUTF();
				out.close();
				in.close();
				socket.close();
			} catch (SocketTimeoutException e) {
				Log.e(TAG, "Connect Time Out");
			} catch (IOException e) {
				Log.e(TAG, "Socket IOException");
			}
		}
		else if (key.equals("@")) {
			try {
				Socket socket = new Socket();
				socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(myPort)), TIMEOUT);
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());
				DataInputStream in = new DataInputStream(socket.getInputStream());
				out.writeUTF("deleteLocalAll");
				num = in.readUTF();
				out.close();
				in.close();
				socket.close();
			} catch (SocketTimeoutException e) {
				Log.e(TAG, "Connect Time Out");
			} catch (IOException e) {
				Log.e(TAG, "Socket IOException");
			}
		}
		else {
			String hKey = "";
			try {
				hKey = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			String successor = find_successor(hKey);
			try {
				Socket socket = new Socket();
				socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());
				DataInputStream in = new DataInputStream(socket.getInputStream());
				out.writeUTF("delete");
				out.writeUTF(key);
				num = in.readUTF();
				out.close();
				socket.close();
			} catch (SocketTimeoutException e) {
				Log.e(TAG, "Connect Time Out");
				return deleteUnderFailure(get_successor(successor), selection, N-1);
			} catch (IOException e) {
				Log.e(TAG, "Socket IOException");
				return deleteUnderFailure(get_successor(successor), selection, N-1);
			}
		}

		return Integer.parseInt(num);
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		String key = (String) values.get(KEY_FIELD);
		String val = (String) values.get(VALUE_FIELD);

		String hKey = "";
		try {
			hKey = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		String successor = find_successor(hKey);
		String write_count = "0";
		try {
			Socket socket = new Socket();
			socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			out.writeUTF("write"); // may send to itself
			out.writeUTF(key);
			out.writeUTF(val);
			write_count = in.readUTF();
			out.close();
			in.close();
			socket.close();

			Log.v("Insert", portTable.get(successor) + " " + key + " " + val);
		} catch (SocketTimeoutException e) {
			Log.e(TAG, "Insert Connect Time Out");
			return insertUnderFailure(uri, values, get_successor(successor), N-1);
		} catch (IOException e) {
			Log.e(TAG, "Insert Socket IOException");
			return insertUnderFailure(uri, values, get_successor(successor), N-1);
		}

		if (Integer.parseInt(write_count)<W) {
			return null;
		}
		else {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(uri.getAuthority());
			uriBuilder.scheme(uri.getScheme());
			uriBuilder.path(key);
			return uriBuilder.build();
		}
	}

	@Override
	public boolean onCreate() {
		myClock = 0;
		obList = new ObjectList();
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		myID = String.valueOf((Integer.parseInt(portStr)));
		try {
			mID = genHash(myID);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		Log.v("MyPort", myPort);

		portTable = new HashMap<String, String>();
		for (int i=0; i<AVD_NO; i++) {
			try {
				portTable.put(genHash(REMOTE_ID[i]), REMOTE_PORT[i]);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		// join
		KEY_RING = new String[AVD_NO];
		try {
			for (int i=0; i<AVD_NO; i++) {
				KEY_RING[i] = genHash(REMOTE_ID[i]);
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Arrays.sort(KEY_RING, new StringComparator());
		indexTable = new HashMap<String, Integer>();
		for (int i=0; i<AVD_NO; i++) {
			indexTable.put(KEY_RING[i], i);
		}
		mSuccessor = get_successor(mID);
		mPredecessor = get_predecessor(mID);

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			serverSocket.setReuseAddress(true);
			//new ResponseTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			//new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			ServerThread st = new ServerThread(serverSocket);
			new Thread(st).start();
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}

		/*SharedPreferences sharedpreferences = getContext().getSharedPreferences("DynamoPrefs", Context.MODE_PRIVATE);
		SharedPreferences.Editor editor = sharedpreferences.edit();
		String state = sharedpreferences.getString("state", "");
		if (state!=null && state.equals("initiated")) {
			RecoverThread rt = new RecoverThread();
			new Thread(rt).start();
		}
		else {
			editor.putString("state", "initiated");
			editor.commit();
		}*/

		return false;
	}

	private String get_successor(String id) {
		return KEY_RING[(indexTable.get(id)+1)%AVD_NO];
	}

	private String get_predecessor(String id) {
		return KEY_RING[(indexTable.get(id)-1+AVD_NO)%AVD_NO];
	}

	private String find_successor(String key) {
		for (int i=0; i<AVD_NO-1; i++) {
			if (isBetween(KEY_RING[i], key, KEY_RING[i+1])) {
				return KEY_RING[i+1];
			}
		}
		return KEY_RING[0];
	}

	// whether b in (a, c]
	private boolean isBetween(String a, String b, String c) {
		//Log.v("Between", a+" "+b+" "+c);

		if (a.compareTo(c)<0) {
			if (b.compareTo(a)>0 && b.compareTo(c)<=0) {
				return true;
			}
		}
		else if (a.compareTo(c)>0) {
			if (b.compareTo(c)>0 && b.compareTo(a)<=0) {
				return false;
			}
			else {
				return true;
			}
		}
		else if (a.compareTo(c)==0) {
			return true;
		}
		return false;
	}

	private class ServerThread implements Runnable {
		ServerSocket serverSocket;
		public ServerThread(ServerSocket _s) {
			serverSocket = _s;
		}

		@Override
		public void run() {

			SharedPreferences sharedpreferences = getContext().getSharedPreferences("DynamoPrefs", Context.MODE_PRIVATE);
			SharedPreferences.Editor editor = sharedpreferences.edit();
			String state = sharedpreferences.getString("state", "");
			if (state!=null && state.equals("initiated")) {
				recover();
			}
			else {
				editor.putString("state", "initiated");
				editor.commit();
			}

			while (true) {
				try {
					Socket server = serverSocket.accept();
					Log.v("Server Task", "Connection Accepted");
					//new ResponseTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, server);
					ResponseThread rt = new ResponseThread(server);
					new Thread(rt).start();
				} catch (IOException e) {
					Log.e(TAG, "ServerThread: Can't receive message");
				}
			}
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, Socket, Void> {

		@Override
		protected Void doInBackground(ServerSocket... params) {
			ServerSocket serverSocket = params[0];
			while (true) {
				try {
					Socket server = serverSocket.accept();
					Log.v("Server Task", "Connection Accepted");
					publishProgress(server);
				} catch (IOException e) {
					Log.e(TAG, "ServerTask: Can't receive message");
				}
			}
		}

		protected void onProgressUpdate(Socket... params) {
			Socket server = params[0];
			Log.v("Server Task", "Server: "+server.toString());
			new ResponseTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, server);
		}
	}

	private class ResponseThread implements Runnable {
		Socket server;

		public ResponseThread(Socket _s) {
			server = _s;
		}

		@Override
		public void run() {
			String request;
			Log.v("Response Task", "Initiated "+server.toString());
			try {
				DataOutputStream out = new DataOutputStream(server.getOutputStream());
				DataInputStream in = new DataInputStream(server.getInputStream());
				request = in.readUTF();

				if (request.equals("write")) {
					responseToWrite(out, in);
				}
				else if (request.equals("replicate")) {
					responseToReplicate(out, in);
				}
				else if (request.equals("read")) {
					//Log.v("Read", "Received read request");
					responseToRead(out, in);
				}
				else if (request.equals("readFromReplica")) {
					responseToReadFromReplica(out, in);
				}
				else if (request.equals("readAll")) {
					responseToReadAll(out, in);
				}
				else if (request.equals("readLocalAll")) {
					responseToReadLocalAll(out, in);
				}
				else if (request.equals("delete")) {
					responseToDelete(out, in);
				}
				else if (request.equals("deleteAll")) {
					responseToDeleteAll(out, in);
				}
				else if (request.equals("deleteLocalAll")) {
					responseToDeleteLocalAll(out, in);
				}
				else if (request.equals("deleteReplica")) {
					responseToDeleteReplica(out, in);
				}
				else if (request.equals("writeUnderFailure")) {
					responseToWriteUnderFailure(out, in);
				}
				else if (request.equals("readUnderFailure")) {
					responseToReadUnderFailure(out, in);
				}
				else if (request.equals("getLocalAll")) {
					responseToGetLocalAll(out, in);
				}
				else if (request.equals("RAUF")) {
					responseToRAUF(out, in);
				}
				else if (request.equals("DUF")) {
					responseToDUF(out, in);
				}
				else {
					Log.e(TAG, "Not supported request");
				}

				out.close();
				in.close();
				server.close();
			} catch (IOException e) {
				Log.e(TAG, "ResponseTask: Can't receive message");
			}
		}
	}

	private class ResponseTask extends AsyncTask<Socket, Void, Void> {

		@Override
		protected Void doInBackground(Socket... params) {
			Log.v("Response Task", "Initiated");
			Socket server = params[0];
			String request;
			Log.v("Response Task", "Initiated "+server.toString());
			try {
				DataOutputStream out = new DataOutputStream(server.getOutputStream());
				DataInputStream in = new DataInputStream(server.getInputStream());
					request = in.readUTF();

				if (request.equals("write")) {
					responseToWrite(out, in);
				}
				else if (request.equals("replicate")) {
					responseToReplicate(out, in);
				}
				else if (request.equals("read")) {
					//Log.v("Read", "Received read request");
					responseToRead(out, in);
				}
				else if (request.equals("readFromReplica")) {
					responseToReadFromReplica(out, in);
				}
				else if (request.equals("readAll")) {
					responseToReadAll(out, in);
				}
				else if (request.equals("readLocalAll")) {
					Log.v("ResponseTask", "Read local all");
					responseToReadLocalAll(out, in);
				}
				out.close();
				in.close();
				server.close();

				//Log.v("Update", "ID=" + mID + " Pred=" + mPredecessor + " Succ=" + mSuccessor);

			} catch (IOException e) {
				Log.e(TAG, "ResponseTask: Can't receive message");
			}
			return null;
		}
	}


/*	private class ResponseTask extends AsyncTask<ServerSocket, Void, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			String request;

			while (true) {
				try {
					//Log.v("ResponseTask", "Start Accept");
					Socket server = serverSocket.accept();
					//Log.v("ResponseTask", "Request Accepted");

					DataOutputStream out = new DataOutputStream(server.getOutputStream());
					DataInputStream in = new DataInputStream(server.getInputStream());

					request = in.readUTF();

					if (request.equals("write")) {
						responseToWrite(out, in);
					}
					else if (request.equals("replicate")) {
						responseToReplicate(out, in);
					}
					else if (request.equals("read")) {
						//Log.v("Read", "Received read request");
						responseToRead(out, in);
					}
					else if (request.equals("readFromReplica")) {
						responseToReadFromReplica(out, in);
					}
					else if (request.equals("readAll")) {
						responseToReadAll(out, in);
					}
					else if (request.equals("readLocalAll")) {
						Log.v("ResponseTask", "Read local all");
						responseToReadLocalAll(out, in);
					}

					out.close();
					in.close();
					server.close();

					//Log.v("Update", "ID=" + mID + " Pred=" + mPredecessor + " Succ=" + mSuccessor);

				} catch (IOException e) {
					Log.e(TAG, "Can't receive message");
				}
			}

			//return null;
		}
	}
*/
	private class StringComparator implements Comparator<String> {

		@Override
		public int compare(String s1, String s2) {
			return s1.compareTo(s2);
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		if (selection==null) return null;

		String key = selection;
		Cursor cursor = null;
		String[] columns = {KEY_FIELD, VALUE_FIELD};

		if (key.equals("*")) {
			MatrixCursor mCursor = new MatrixCursor(columns);
			try {
				Socket socket = new Socket();
				socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(myPort)), TIMEOUT);
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());
				DataInputStream in = new DataInputStream(socket.getInputStream());
				out.writeUTF("readAll");

				String allKey = in.readUTF();
				while (!allKey.equals("EOF")) {
					String allVal = in.readUTF();
					mCursor.addRow(new Object[]{allKey, allVal});
					allKey = in.readUTF();
				}

				out.close();
				in.close();
				socket.close();
			} catch (SocketTimeoutException e) {
				Log.e(TAG, "Connect Time Out");
			} catch (IOException e) {
				Log.e(TAG, "Socket IOException");
			}
			cursor = mCursor;
		}
		else if (key.equals("@")) {
			MatrixCursor mCursor = new MatrixCursor(columns);
			try {
				Socket socket = new Socket();
				socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(myPort)), TIMEOUT);
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());
				DataInputStream in = new DataInputStream(socket.getInputStream());
				out.writeUTF("getLocalAll");

				String localAllKey = in.readUTF();
				while (!localAllKey.equals("EOF")) {
					String allVal = in.readUTF();
					mCursor.addRow(new Object[]{localAllKey, allVal});
					localAllKey = in.readUTF();
				}

				out.close();
				in.close();
				socket.close();
			} catch (SocketTimeoutException e) {
				Log.e(TAG, "Connect Time Out");
			} catch (IOException e) {
				Log.e(TAG, "Socket IOException");
			}
			cursor = mCursor;
		}
		else {

			MatrixCursor mCursor = new MatrixCursor(columns);
			String hKey = "";
			try {
				hKey = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			String successor = find_successor(hKey);
			String read_count = "0";

			try {
				Log.v("Query", "Begin to connect "+portTable.get(successor));

				Socket socket = new Socket();
				socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());
				DataInputStream in = new DataInputStream(socket.getInputStream());
				out.writeUTF("read"); // may send to itself
				out.writeUTF(key);

				read_count = in.readUTF();
				if (Integer.parseInt(read_count) >= R) {
					String val = in.readUTF();
					mCursor.addRow(new Object[]{key, val});
					cursor = mCursor;
				}
				Log.v("Query", "Return cursor");
				out.close();
				in.close();
				socket.close();
			} catch (SocketTimeoutException e) {
				Log.e(TAG, "Connect Time Out");
				return queryUnderFailure(uri, selection, get_successor(successor), N-1);
			} catch (IOException e) {
				Log.e(TAG, "Socket IOException");
				return queryUnderFailure(uri, selection, get_successor(successor), N-1);
			}
		}
		Log.v("Read", selection);
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	private String reconciliation(ArrayList<ObjectVersioning> obvs) {
		return obvs.get(0).val;
	}

	private void responseToWrite(DataOutputStream out, DataInputStream in) {
		try {
			String key = in.readUTF();
			String val = in.readUTF();

			myClock++;
			obList.writeObject(key, val, myID, myClock.toString());

			String successor = get_successor(mID);
			Integer write_count = 1;
			for (int i = 0; i < N - 1; i++) {
				try {
					Socket socketR = new Socket();
					socketR.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
					DataOutputStream outR = new DataOutputStream(socketR.getOutputStream());
					//DataInputStream inR = new DataInputStream(socketR.getInputStream());
					outR.writeUTF("replicate");
					outR.writeUTF(key);
					outR.writeUTF(val);
					outR.writeUTF(myID);
					outR.writeUTF(myClock.toString());
					outR.close();
					socketR.close();
					write_count++;
				} catch (SocketTimeoutException e) {
					Log.e(TAG, "Connect Time Out");
				} catch (IOException e) {
					Log.e(TAG, "Socket IOException");
				}
				successor = get_successor(successor);
			}
			out.writeUTF(write_count.toString());
		} catch (IOException e) {
		Log.e(TAG, "Can't receive message");
		}
	}

	private void responseToReplicate(DataOutputStream out, DataInputStream in) {
		try {
			String key = in.readUTF();
			String val = in.readUTF();
			String node = in.readUTF();
			String clock = in.readUTF();
			obList.writeObject(key, val, node, clock);
		} catch (IOException e) {
			Log.e(TAG, "Can't receive message");
		}
	}

	private void responseToRead(DataOutputStream out, DataInputStream in) {
		try {
			ArrayList<ObjectVersioning> obvs = new ArrayList<ObjectVersioning>();
			String key = in.readUTF();
			ObjectVersioning obv = obList.readObject(key);
			if (obv==null) {
				out.writeUTF("0");
			}
			else {
				obvs.add(obv);
				String successor = get_successor(mID);
				Integer read_count = 1;
				for (int i = 0; i < N - 1; i++) {

					try {
						Log.v("Read", "Send read from replica request");
						Socket socketRR = new Socket();
						socketRR.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
						DataOutputStream outRR = new DataOutputStream(socketRR.getOutputStream());
						DataInputStream inRR = new DataInputStream(socketRR.getInputStream());

						outRR.writeUTF("readFromReplica");
						outRR.writeUTF(key);

						// receive object versioning
						obv = new ObjectVersioning();
						obv.key = key;
						obv.val = inRR.readUTF();

						if (obv.val.equals("null")) {
							successor = get_successor(successor);
							//throw new IOException();
							continue;
						}

						Log.v("Read", "Read from replica, val: " + obv.val);

						Integer numberOfVC = Integer.parseInt(inRR.readUTF());

						Log.v("Read", "Read from replica, numberOfVC: " + numberOfVC.toString());

						for (int j = 0; j < numberOfVC; j++) {
							String node = inRR.readUTF();
							String clock = inRR.readUTF();
							obv.vc.updateClock(node, clock);
						}
						obvs.add(obv);

						outRR.close();
						inRR.close();
						socketRR.close();
						read_count++;
					} catch (SocketTimeoutException e) {
						Log.e(TAG, "Connect Time Out");
					} catch (IOException e) {
						Log.e(TAG, "Socket IOException");
					}
					successor = get_successor(successor);
				}

				Log.v("Read", "Read count " + read_count.toString());

				out.writeUTF(read_count.toString());
				if (read_count >= R) {
					String r_val = reconciliation(obvs);
					out.writeUTF(r_val);
				}
			}
		} catch (IOException e) {
			Log.e(TAG, "Can't receive message");
		}
	}

	private void responseToReadFromReplica(DataOutputStream out, DataInputStream in) {
		try {
			String key = in.readUTF();
			//Log.v("Read", "Received read from replica request " + key);
			ObjectVersioning obv = obList.readObject(key);
			if (obv==null) {
				out.writeUTF("null");
				return;
			}
			//Log.v("readFromReplica val: ", obv.val);
			out.writeUTF(obv.val);
			Integer numberOfVC = obv.vc.clocks.size();
			//Log.v("readFromReplica", "numberOfVC: "+obv.val);
			out.writeUTF(numberOfVC.toString());
			Iterator<Map.Entry<String, String>> it = obv.vc.clocks.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, String> entry = it.next();
				String node = entry.getKey();
				String clock = entry.getValue();
				out.writeUTF(node);
				out.writeUTF(clock);
			}
		} catch (IOException e) {
			Log.e(TAG, "Can't receive message");
		}
	}

	private void responseToReadAll(DataOutputStream out, DataInputStream in) {
		// for responsible keys, read from itself and replica, and reconciliation
		// then ask other nodes to do the same thing
		HashMap<String, String> results = new HashMap<String, String>();
		// for responsible keys
		Iterator<String> it = obList.objects.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			String hKey = "";
			try {
				hKey = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			if (!isBetween(mPredecessor, hKey, mID)) continue;
			ArrayList<ObjectVersioning> obvs = new ArrayList<ObjectVersioning>();
			ObjectVersioning obv = obList.readObject(key);
			obvs.add(obv);
			String successor = get_successor(mID);
			Integer read_count = 1;
			for (int i = 0; i < N - 1; i++) {
				try {
					Socket socketRR = new Socket();
					socketRR.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
					DataOutputStream outRR = new DataOutputStream(socketRR.getOutputStream());
					DataInputStream inRR = new DataInputStream(socketRR.getInputStream());
					outRR.writeUTF("readFromReplica");
					outRR.writeUTF(key);
					// receive object versioning
					obv = new ObjectVersioning();
					obv.key = key;
					obv.val = inRR.readUTF();
					if (obv.val.equals("null")) {
						successor = get_successor(successor);
						continue;
					}
					Integer numberOfVC = Integer.parseInt(inRR.readUTF());
					for (int j = 0; j < numberOfVC; j++) {
						String node = inRR.readUTF();
						String clock = inRR.readUTF();
						obv.vc.updateClock(node, clock);
					}
					obvs.add(obv);
					outRR.close();
					inRR.close();
					socketRR.close();
					read_count++;
				} catch (SocketTimeoutException e) {
					Log.e(TAG, "Connect Time Out");
				} catch (IOException e) {
					Log.e(TAG, "Socket IOException");
				}
				successor = get_successor(successor);
			}
			if (read_count >= R) {
				String r_val = reconciliation(obvs);
				results.put(key, r_val);
			}
		}
		Log.v("ReadAll", "Local No.: "+String.valueOf(results.size()));
		// for other nodes
		String successor = get_successor(mID);
		while (!successor.equals(mID)) {
			Log.v("ReadAll", "Successor: "+successor);
			try {
				Socket socketON = new Socket();
				socketON.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);

				Log.v("ReadAll", portTable.get(successor));

				DataOutputStream outON = new DataOutputStream(socketON.getOutputStream());
				DataInputStream inON = new DataInputStream(socketON.getInputStream());
				outON.writeUTF("readLocalAll");
				String keyON = inON.readUTF();

				Log.v("ReadAll", "keyOn="+keyON);

				while (!keyON.equals("EOF")) {
					String valON = inON.readUTF();
					results.put(keyON, valON);
					keyON = inON.readUTF();

					Log.v("ReadAll", "keyOn="+keyON);

				}
				outON.close();
				inON.close();
				socketON.close();
			} catch (SocketTimeoutException e) {
				Log.e(TAG, "Connect Time Out");
				readAllUnderFailure(get_successor(successor), N-1, get_predecessor(successor), successor, results);
			} catch (IOException e) {
				Log.e(TAG, "Socket IOException");
				readAllUnderFailure(get_successor(successor), N - 1, get_predecessor(successor), successor, results);
			}
			successor = get_successor(successor);
		}
		Log.v("ReadAll", "Total No.: "+String.valueOf(results.size()));
		Iterator<Map.Entry<String, String>> itr = results.entrySet().iterator();
		try {
			while (itr.hasNext()) {
				Map.Entry<String, String> ent = itr.next();
				out.writeUTF(ent.getKey());
				out.writeUTF(ent.getValue());
			}
			out.writeUTF("EOF");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void responseToReadLocalAll(DataOutputStream out, DataInputStream in) {
		HashMap<String, String> results = new HashMap<String, String>();
		Iterator<String> it = obList.objects.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			String hKey = "";
			try {
				hKey = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			if (!isBetween(mPredecessor, hKey, mID)) continue;
			ArrayList<ObjectVersioning> obvs = new ArrayList<ObjectVersioning>();
			ObjectVersioning obv = obList.readObject(key);
			obvs.add(obv);

			//////////////////////////
			//results.put(key, obv.val);
			//////////////////////////

			String successor = get_successor(mID);
			Integer read_count = 1;
			for (int i = 0; i < N - 1; i++) {
				try {
					Socket socketRR = new Socket();
					socketRR.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
					DataOutputStream outRR = new DataOutputStream(socketRR.getOutputStream());
					DataInputStream inRR = new DataInputStream(socketRR.getInputStream());
					outRR.writeUTF("readFromReplica");
					outRR.writeUTF(key);
					// receive object versioning
					obv = new ObjectVersioning();
					obv.key = key;
					obv.val = inRR.readUTF();
					if (obv.val.equals("null")) {
						successor = get_successor(successor);
						continue;
					}
					Integer numberOfVC = Integer.parseInt(inRR.readUTF());
					for (int j = 0; j < numberOfVC; j++) {
						String node = inRR.readUTF();
						String clock = inRR.readUTF();
						obv.vc.updateClock(node, clock);
					}
					obvs.add(obv);
					outRR.close();
					inRR.close();
					socketRR.close();
					read_count++;
				} catch (SocketTimeoutException e) {
					Log.e(TAG, "Connect Time Out");
				} catch (IOException e) {
					Log.e(TAG, "Socket IOException");
				}
				successor = get_successor(successor);
			}
			if (read_count >= R) {
				String r_val = reconciliation(obvs);
				results.put(key, r_val);
			}
		}
		Iterator<Map.Entry<String, String>> itr = results.entrySet().iterator();
		try {
			while (itr.hasNext()) {
				Map.Entry<String, String> ent = itr.next();
				out.writeUTF(ent.getKey());
				out.writeUTF(ent.getValue());
			}
			out.writeUTF("EOF");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void responseToDelete(DataOutputStream out, DataInputStream in) {
		try {
			String key = in.readUTF();
			if (!obList.isContained(key)) {
				out.writeUTF("0");
			}
			else {
				obList.deleteObject(key);
				String successor = get_successor(mID);
				for (int i=0; i<N-1; i++) {
					Socket socketD = new Socket();
					socketD.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
					DataOutputStream outD = new DataOutputStream(socketD.getOutputStream());
					outD.writeUTF("deleteReplica");
					outD.writeUTF(key);
					outD.close();
					socketD.close();
					successor = get_successor(successor);
				}
				out.writeUTF("1");
			}
		} catch (SocketTimeoutException e) {
			Log.e(TAG, "Connect Time Out");
		} catch (IOException e) {
			Log.e(TAG, "Socket IOException");
		}
	}

	private void responseToDeleteAll(DataOutputStream out, DataInputStream in) {
		Iterator<String> it = obList.objects.keySet().iterator();
		Integer count = 0;
		while (it.hasNext()) {
			String key = it.next();
			String hKey = "";
			try {
				hKey = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			if (!isBetween(mPredecessor, hKey, mID)) continue;
			obList.deleteObject(key);
			count++;
			String successor = get_successor(mID);
			for (int i = 0; i < N - 1; i++) {
				try {
					Socket socketD = new Socket();
					socketD.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
					DataOutputStream outD = new DataOutputStream(socketD.getOutputStream());
					outD.writeUTF("deleteReplica");
					outD.writeUTF(key);
					outD.close();
					socketD.close();
				} catch (SocketTimeoutException e) {
					Log.e(TAG, "Connect Time Out");
				} catch (IOException e) {
					Log.e(TAG, "Socket IOException");
				}
				successor = get_successor(successor);
			}
		}

		String successor = get_successor(mID);
		while (!successor.equals(mID)) {
			Log.v("DeleteAll", "Successor: "+successor);
			try {
				Socket socketON = new Socket();
				socketON.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);

				DataOutputStream outON = new DataOutputStream(socketON.getOutputStream());
				DataInputStream inON = new DataInputStream(socketON.getInputStream());
				outON.writeUTF("deleteLocalAll");
				String num = inON.readUTF();
				count += Integer.parseInt(num);
				outON.close();
				inON.close();
				socketON.close();
			} catch (SocketTimeoutException e) {
				Log.e(TAG, "Connect Time Out");
			} catch (IOException e) {
				Log.e(TAG, "Socket IOException");
			}
			successor = get_successor(successor);
		}

		try {
			out.writeUTF(count.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void responseToDeleteLocalAll(DataOutputStream out, DataInputStream in) {
		Iterator<String> it = obList.objects.keySet().iterator();
		Integer count = 0;
		while (it.hasNext()) {
			String key = it.next();
			String hKey = "";
			try {
				hKey = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			if (!isBetween(mPredecessor, hKey, mID)) continue;
			obList.deleteObject(key);
			count++;
			String successor = get_successor(mID);
			for (int i = 0; i < N - 1; i++) {
				try {
					Socket socketD = new Socket();
					socketD.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
					DataOutputStream outD = new DataOutputStream(socketD.getOutputStream());
					outD.writeUTF("deleteReplica");
					outD.writeUTF(key);
					outD.close();
					socketD.close();
				} catch (SocketTimeoutException e) {
					Log.e(TAG, "Connect Time Out");
				} catch (IOException e) {
					Log.e(TAG, "Socket IOException");
				}
				successor = get_successor(successor);
			}
		}
		try {
			out.writeUTF(count.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void responseToDeleteReplica(DataOutputStream out, DataInputStream in) {
		String key = null;
		try {
			key = in.readUTF();
		} catch (IOException e) {
			e.printStackTrace();
		}
		obList.deleteObject(key);
	}

	private Uri insertUnderFailure(Uri uri, ContentValues values, String node, Integer numOfReplica) {
		String key = (String) values.get(KEY_FIELD);
		String val = (String) values.get(VALUE_FIELD);

		String write_count = "0";
		try {
			Socket socket = new Socket();
			socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(node))), TIMEOUT);
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			out.writeUTF("writeUnderFailure"); // may send to itself
			out.writeUTF(key);
			out.writeUTF(val);
			out.writeUTF(numOfReplica.toString());
			write_count = in.readUTF();
			out.close();
			in.close();
			socket.close();

			Log.v("Insert", portTable.get(node) + " " + key + " " + val);
		} catch (SocketTimeoutException e) {
			Log.e(TAG, "Insert Connect Time Out");
			return insertUnderFailure(uri, values, get_successor(node), numOfReplica-1);
		} catch (IOException e) {
			Log.e(TAG, "Insert Socket IOException");
		}

		if (Integer.parseInt(write_count)<W) {
			return null;
		}
		else {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(uri.getAuthority());
			uriBuilder.scheme(uri.getScheme());
			uriBuilder.path(key);
			return uriBuilder.build();
		}
	}

	private void responseToWriteUnderFailure(DataOutputStream out, DataInputStream in) {
		try {
			String key = in.readUTF();
			String val = in.readUTF();
			Integer numOfReplica = Integer.parseInt(in.readUTF());

			myClock++;
			obList.writeObject(key, val, myID, myClock.toString());

			String successor = get_successor(mID);
			Integer write_count = 1;
			for (int i = 0; i < numOfReplica - 1; i++) {
				try {
					Socket socketR = new Socket();
					socketR.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
					DataOutputStream outR = new DataOutputStream(socketR.getOutputStream());
					//DataInputStream inR = new DataInputStream(socketR.getInputStream());
					outR.writeUTF("replicate");
					outR.writeUTF(key);
					outR.writeUTF(val);
					outR.writeUTF(myID);
					outR.writeUTF(myClock.toString());
					outR.close();
					socketR.close();
					write_count++;
				} catch (SocketTimeoutException e) {
					Log.e(TAG, "Connect Time Out");
				} catch (IOException e) {
					Log.e(TAG, "Socket IOException");
				}
				successor = get_successor(successor);
			}
			out.writeUTF(write_count.toString());
		} catch (IOException e) {
			Log.e(TAG, "Can't receive message");
		}
	}

	private void responseToReadUnderFailure(DataOutputStream out, DataInputStream in) {
		try {
			ArrayList<ObjectVersioning> obvs = new ArrayList<ObjectVersioning>();
			String key = in.readUTF();
			Integer numOfReplica = Integer.parseInt(in.readUTF());

			ObjectVersioning obv = obList.readObject(key);
			if (obv==null) {
				out.writeUTF("0");
			}
			else {
				obvs.add(obv);
				String successor = get_successor(mID);
				Integer read_count = 1;
				for (int i = 0; i < numOfReplica - 1; i++) {

					try {
						//Log.v("Read", "Send read from replica request");
						Socket socketRR = new Socket();
						socketRR.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
						DataOutputStream outRR = new DataOutputStream(socketRR.getOutputStream());
						DataInputStream inRR = new DataInputStream(socketRR.getInputStream());

						outRR.writeUTF("readFromReplica");
						outRR.writeUTF(key);

						// receive object versioning
						obv = new ObjectVersioning();
						obv.key = key;
						obv.val = inRR.readUTF();
						if (obv.val.equals("null")) {
							successor = get_successor(successor);
							continue;
						}
						//Log.v("Read", "Read from replica, val: " + obv.val);

						Integer numberOfVC = Integer.parseInt(inRR.readUTF());

						//Log.v("Read", "Read from replica, numberOfVC: " + numberOfVC.toString());

						for (int j = 0; j < numberOfVC; j++) {
							String node = inRR.readUTF();
							String clock = inRR.readUTF();
							obv.vc.updateClock(node, clock);
						}
						obvs.add(obv);

						outRR.close();
						inRR.close();
						socketRR.close();
						read_count++;
					} catch (SocketTimeoutException e) {
						Log.e(TAG, "Connect Time Out");
					} catch (IOException e) {
						Log.e(TAG, "Socket IOException");
					}
					successor = get_successor(successor);
				}

				//Log.v("Read", "Read count " + read_count.toString());

				out.writeUTF(read_count.toString());
				if (read_count >= R) {
					String r_val = reconciliation(obvs);
					out.writeUTF(r_val);
				}
			}
		} catch (IOException e) {
			Log.e(TAG, "Can't receive message");
		}
	}

	private void responseToGetLocalAll(DataOutputStream out, DataInputStream in) {
		HashMap<String, String> results = new HashMap<String, String>();
		Iterator<String> it = obList.objects.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			String hKey = "";
			try {
				hKey = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			ObjectVersioning obv = obList.readObject(key);
			results.put(key, obv.val);
		}
		Iterator<Map.Entry<String, String>> itr = results.entrySet().iterator();
		try {
			while (itr.hasNext()) {
				Map.Entry<String, String> ent = itr.next();
				out.writeUTF(ent.getKey());
				out.writeUTF(ent.getValue());
			}
			out.writeUTF("EOF");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Cursor queryUnderFailure(Uri uri, String selection, String node, Integer numOfReplica) {
		String key = selection;
		Cursor cursor = null;
		String[] columns = {KEY_FIELD, VALUE_FIELD};

		MatrixCursor mCursor = new MatrixCursor(columns);
		String read_count = "0";

		try {
			Log.v("Query", "Begin to connect "+portTable.get(node));

			Socket socket = new Socket();
			socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(node))), TIMEOUT);
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			out.writeUTF("readUnderFailure"); // may send to itself
			out.writeUTF(key);
			out.writeUTF(numOfReplica.toString());

			read_count = in.readUTF();
			if (Integer.parseInt(read_count) >= R) {
				String val = in.readUTF();
				mCursor.addRow(new Object[]{key, val});
				cursor = mCursor;
			}
			Log.v("Query", "Return cursor");
			out.close();
			in.close();
			socket.close();
		} catch (SocketTimeoutException e) {
			Log.e(TAG, "Connect Time Out");
			return queryUnderFailure(uri, selection, get_successor(node), numOfReplica-1);
		} catch (IOException e) {
			Log.e(TAG, "Socket IOException");
			return queryUnderFailure(uri, selection, get_successor(node), numOfReplica-1);
		}
		Log.v("Read", selection);
		return cursor;
	}

	private void readAllUnderFailure(String node, Integer numOfReplica, String lb, String ub, HashMap<String, String> results) {
		try {
			Socket socketON = new Socket();
			socketON.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(node))), TIMEOUT);

			DataOutputStream outON = new DataOutputStream(socketON.getOutputStream());
			DataInputStream inON = new DataInputStream(socketON.getInputStream());
			outON.writeUTF("RAUF");
			outON.writeUTF(numOfReplica.toString());
			outON.writeUTF(lb);
			outON.writeUTF(ub);

			String keyON = inON.readUTF();
			while (!keyON.equals("EOF")) {
				String valON = inON.readUTF();
				results.put(keyON, valON);
				keyON = inON.readUTF();
			}
			outON.close();
			inON.close();
			socketON.close();
		} catch (SocketTimeoutException e) {
			Log.e(TAG, "Connect Time Out");
			readAllUnderFailure(get_successor(node), numOfReplica - 1, lb, ub, results);
		} catch (IOException e) {
			Log.e(TAG, "Socket IOException");
			readAllUnderFailure(get_successor(node), numOfReplica - 1, lb, ub, results);
		}
	}

	private void responseToRAUF(DataOutputStream out, DataInputStream in) {
		Integer numOfReplica = 0;
		String lb = "";
		String ub = "";
		try {
			numOfReplica = Integer.parseInt(in.readUTF());
			lb = in.readUTF();
			ub = in.readUTF();
		} catch (IOException e) {
			e.printStackTrace();
		}

		HashMap<String, String> results = new HashMap<String, String>();
		Iterator<String> it = obList.objects.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			String hKey = "";
			try {
				hKey = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			if (!isBetween(lb, hKey, ub)) continue;
			ArrayList<ObjectVersioning> obvs = new ArrayList<ObjectVersioning>();
			ObjectVersioning obv = obList.readObject(key);
			obvs.add(obv);

			String successor = get_successor(mID);
			Integer read_count = 1;
			for (int i = 0; i < numOfReplica - 1; i++) {
				try {
					Socket socketRR = new Socket();
					socketRR.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
					DataOutputStream outRR = new DataOutputStream(socketRR.getOutputStream());
					DataInputStream inRR = new DataInputStream(socketRR.getInputStream());
					outRR.writeUTF("readFromReplica");
					outRR.writeUTF(key);
					// receive object versioning
					obv = new ObjectVersioning();
					obv.key = key;
					obv.val = inRR.readUTF();
					if (obv.val.equals("null")) {
						successor = get_successor(successor);
						continue;
					}
					Integer numberOfVC = Integer.parseInt(inRR.readUTF());
					for (int j = 0; j < numberOfVC; j++) {
						String node = inRR.readUTF();
						String clock = inRR.readUTF();
						obv.vc.updateClock(node, clock);
					}
					obvs.add(obv);
					outRR.close();
					inRR.close();
					socketRR.close();
					read_count++;
				} catch (SocketTimeoutException e) {
					Log.e(TAG, "Connect Time Out");
				} catch (IOException e) {
					Log.e(TAG, "Socket IOException");
				}
				successor = get_successor(successor);
			}
			if (read_count >= R) {
				String r_val = reconciliation(obvs);
				results.put(key, r_val);
			}
		}
		Iterator<Map.Entry<String, String>> itr = results.entrySet().iterator();
		try {
			while (itr.hasNext()) {
				Map.Entry<String, String> ent = itr.next();
				out.writeUTF(ent.getKey());
				out.writeUTF(ent.getValue());
			}
			out.writeUTF("EOF");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Integer deleteUnderFailure(String node, String key, Integer numOfReplica) {
		String num = "0";
		try {
			Socket socket = new Socket();
			socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(node))), TIMEOUT);
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			out.writeUTF("DUF");
			out.writeUTF(key);
			out.writeUTF(numOfReplica.toString());
			num = in.readUTF();
			out.close();
			socket.close();
		} catch (SocketTimeoutException e) {
			Log.e(TAG, "Connect Time Out");
			return deleteUnderFailure(get_successor(node), key, numOfReplica-1);
		} catch (IOException e) {
			Log.e(TAG, "Socket IOException");
			return deleteUnderFailure(get_successor(node), key, numOfReplica-1);
		}
		return Integer.parseInt(num);
	}

	private void responseToDUF(DataOutputStream out, DataInputStream in) {
		try {
			String key = in.readUTF();
			Integer numOfReplica = Integer.parseInt(in.readUTF());
			if (!obList.isContained(key)) {
				out.writeUTF("0");
			}
			else {
				obList.deleteObject(key);
				String successor = get_successor(mID);
				for (int i=0; i<numOfReplica-1; i++) {
					Socket socketD = new Socket();
					socketD.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
					DataOutputStream outD = new DataOutputStream(socketD.getOutputStream());
					outD.writeUTF("deleteReplica");
					outD.writeUTF(key);
					outD.close();
					socketD.close();
					successor = get_successor(successor);
				}
				out.writeUTF("1");
			}
		} catch (SocketTimeoutException e) {
			Log.e(TAG, "Connect Time Out");
		} catch (IOException e) {
			Log.e(TAG, "Socket IOException");
		}
	}

	private class RecoverThread implements Runnable {

		@Override
		public void run() {
			if (mPredecessor.equals(mID) || mSuccessor.equals(mID)) return;
			Log.v("Recover", "Start");

			Cursor allFromPred = null;
			MatrixCursor mCursor;
			String[] columns = {KEY_FIELD, VALUE_FIELD};

			// ask predecessor for all replicas
			mCursor = new MatrixCursor(columns);
			try {
				Socket socket = new Socket();
				socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(mPredecessor))), TIMEOUT);
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());
				DataInputStream in = new DataInputStream(socket.getInputStream());
				out.writeUTF("getLocalAll");

				String localAllKey = in.readUTF();
				while (!localAllKey.equals("EOF")) {
					String allVal = in.readUTF();
					mCursor.addRow(new Object[]{localAllKey, allVal});
					localAllKey = in.readUTF();
				}

				out.close();
				in.close();
				socket.close();

				allFromPred = mCursor;
			} catch (SocketTimeoutException e) {
				Log.e(TAG, "Connect Time Out");
			} catch (IOException e) {
				Log.e(TAG, "Socket IOException");
			}

			if (allFromPred != null) {
				int keyIndex = allFromPred.getColumnIndex(KEY_FIELD);
				int valueIndex = allFromPred.getColumnIndex(VALUE_FIELD);
				allFromPred.moveToFirst();
				while (!allFromPred.isAfterLast()) {
					String returnKey = allFromPred.getString(keyIndex);
					String returnValue = allFromPred.getString(valueIndex);
					String hKey = "";
					try {
						hKey = genHash(returnKey);
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}
					String bound = get_predecessor(mID);
					for (int i = 0; i < N - 1 && !bound.equals(mID); i++) {
						bound = get_predecessor(bound);
					}
					if (isBetween(bound, hKey, mID)) {
						myClock++;
						obList.writeObject(returnKey, returnValue, myID, myClock.toString());
					}
					allFromPred.moveToNext();
				}
			}

			// ask successor for all responsible keys
			Cursor allFromSucc = null;
			mCursor = new MatrixCursor(columns);
			try {
				Socket socket = new Socket();
				socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(mSuccessor))), TIMEOUT);
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());
				DataInputStream in = new DataInputStream(socket.getInputStream());
				out.writeUTF("getLocalAll");

				String localAllKey = in.readUTF();
				while (!localAllKey.equals("EOF")) {
					String allVal = in.readUTF();
					mCursor.addRow(new Object[]{localAllKey, allVal});
					localAllKey = in.readUTF();
				}

				out.close();
				in.close();
				socket.close();

				allFromSucc = mCursor;
			} catch (SocketTimeoutException e) {
				Log.e(TAG, "Connect Time Out");
			} catch (IOException e) {
				Log.e(TAG, "Socket IOException");
			}

			if (allFromSucc != null) {
				int keyIndex = allFromSucc.getColumnIndex(KEY_FIELD);
				int valueIndex = allFromSucc.getColumnIndex(VALUE_FIELD);
				allFromSucc.moveToFirst();
				while (!allFromSucc.isAfterLast()) {
					String returnKey = allFromSucc.getString(keyIndex);
					String returnValue = allFromSucc.getString(valueIndex);
					String hKey = "";
					try {
						hKey = genHash(returnKey);
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}
					if (isBetween(mPredecessor, hKey, mID)) {
						myClock++;
						obList.writeObject(returnKey, returnValue, myID, myClock.toString());
					}
					allFromSucc.moveToNext();
				}
			}
		}
	}

	private void recover() {

		if (mPredecessor.equals(mID) || mSuccessor.equals(mID)) return;
		Log.v("Recover", "Start");

		Cursor allFromPred = null;
		MatrixCursor mCursor;
		String[] columns = {KEY_FIELD, VALUE_FIELD};

		// ask predecessor for all replicas
		mCursor = new MatrixCursor(columns);
		try {
			Socket socket = new Socket();
			socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(mPredecessor))), TIMEOUT);
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			out.writeUTF("getLocalAll");

			String localAllKey = in.readUTF();
			while (!localAllKey.equals("EOF")) {
				String allVal = in.readUTF();
				mCursor.addRow(new Object[]{localAllKey, allVal});
				localAllKey = in.readUTF();
			}

			out.close();
			in.close();
			socket.close();

			allFromPred = mCursor;
		} catch (SocketTimeoutException e) {
			Log.e(TAG, "Connect Time Out");
		} catch (IOException e) {
			Log.e(TAG, "Socket IOException");
		}

		if (allFromPred != null) {
			int keyIndex = allFromPred.getColumnIndex(KEY_FIELD);
			int valueIndex = allFromPred.getColumnIndex(VALUE_FIELD);
			allFromPred.moveToFirst();
			while (!allFromPred.isAfterLast()) {
				String returnKey = allFromPred.getString(keyIndex);
				String returnValue = allFromPred.getString(valueIndex);
				String hKey = "";
				try {
					hKey = genHash(returnKey);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				String bound = get_predecessor(mID);
				for (int i = 0; i < N - 1 && !bound.equals(mID); i++) {
					bound = get_predecessor(bound);
				}
				if (isBetween(bound, hKey, mID)) {
					myClock++;
					obList.writeObject(returnKey, returnValue, myID, myClock.toString());
				}
				allFromPred.moveToNext();
			}
		}

		// ask successor for all responsible keys
		Cursor allFromSucc = null;
		mCursor = new MatrixCursor(columns);
		try {
			Socket socket = new Socket();
			socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(mSuccessor))), TIMEOUT);
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			DataInputStream in = new DataInputStream(socket.getInputStream());
			out.writeUTF("getLocalAll");

			String localAllKey = in.readUTF();
			while (!localAllKey.equals("EOF")) {
				String allVal = in.readUTF();
				mCursor.addRow(new Object[]{localAllKey, allVal});
				localAllKey = in.readUTF();
			}

			out.close();
			in.close();
			socket.close();

			allFromSucc = mCursor;
		} catch (SocketTimeoutException e) {
			Log.e(TAG, "Connect Time Out");
		} catch (IOException e) {
			Log.e(TAG, "Socket IOException");
		}

		if (allFromSucc != null) {
			int keyIndex = allFromSucc.getColumnIndex(KEY_FIELD);
			int valueIndex = allFromSucc.getColumnIndex(VALUE_FIELD);
			allFromSucc.moveToFirst();
			while (!allFromSucc.isAfterLast()) {
				String returnKey = allFromSucc.getString(keyIndex);
				String returnValue = allFromSucc.getString(valueIndex);
				String hKey = "";
				try {
					hKey = genHash(returnKey);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				if (isBetween(mPredecessor, hKey, mID)) {
					myClock++;
					obList.writeObject(returnKey, returnValue, myID, myClock.toString());
				}
				allFromSucc.moveToNext();
			}
		}
	}
}
