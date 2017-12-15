package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String [] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
    static final String [] REMOTE_ID = {"5554", "5556", "5558", "5560", "5562"};
    static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final int AVD_NO = 5;
    private String myPort;
    private String myID;
    private static final int TIMEOUT = 10000;

    private String mID;
    private String mSuccessor;
    private String mPredecessor;

    private HashMap<String, String> portTable;

    private static final String MINID = "0000000000000000000000000000000000000000";
    private static final String MAXID = "ffffffffffffffffffffffffffffffffffffffff";

    private ArrayList<String> keyArray;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String key = selection;
        if (key==null) return 0;

        if (key.equals("*")) {
            Iterator<String> it = keyArray.iterator();
            while (it.hasNext()) {
                String atKey = it.next();
                File file = new File(getContext().getFilesDir().getAbsolutePath(), atKey);
                file.delete();
                it.remove();
            }
            if (!mSuccessor.equals(mID)) {
                try {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(mSuccessor))), TIMEOUT);

                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());

                    out.writeUTF("deleteAll");
                    out.writeUTF(mID);

                    out.close();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        else if (key.equals("@")) {
            Iterator<String> it = keyArray.iterator();
            while (it.hasNext()) {
                String atKey = it.next();
                File file = new File(getContext().getFilesDir().getAbsolutePath(), atKey);
                file.delete();
                it.remove();
            }
        }
        else {
            String hKey = "";
            try {
                hKey = genHash(key);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            String predecessor = find_predecessor(hKey);
            String successor = get_sucessor(predecessor);
            if (successor.equals(mID)) {
                keyArray.remove(key);
                File file = new File(getContext().getFilesDir().getAbsolutePath(), key);
                file.delete();
            }
            else {
                try {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());

                    out.writeUTF("delete");
                    out.writeUTF(key);

                    out.close();
                    socket.close();

                    Log.v("Delete", portTable.get(successor) + " " + key);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        Log.v("Delete", selection);
        return 0;
    }

    @Override
    public String getType(Uri uri) {

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

        String predecessor = find_predecessor(hKey);
        String successor = get_sucessor(predecessor);

        //new InsertTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portTable.get(successor), key, val);

        String port = portTable.get(successor);

        if (port.equals(myPort)) {
            keyArray.add(key);
            File file = new File(getContext().getFilesDir().getAbsolutePath(), key);
            if (file.exists() && file.isFile()) file.delete();
            try {
                FileWriter fWrite = new FileWriter(file);
                fWrite.write(val);
                fWrite.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            Log.v("Insert", port+" "+key+" "+val);
            return null;
        }

        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(port)), TIMEOUT);

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            out.writeUTF("insert");
            out.writeUTF(key);
            out.writeUTF(val);

            out.close();
            socket.close();

            Log.v("Insert", port + " " + key + " " + val);
        } catch (IOException e) {
            e.printStackTrace();
        }


        return null;
    }

    /*
    private class InsertTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... strs) {
            String port = strs[0];
            String key = strs[1];
            String val = strs[2];

            if (port.equals(myPort)) {
                File file = new File(getContext().getFilesDir().getAbsolutePath(), key);

                if (file.exists() && file.isFile()) file.delete();

                try {
                    FileWriter fWrite = new FileWriter(file);
                    fWrite.write(val);
                    fWrite.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                Log.v("insert", port+" "+key+" "+val);
                return null;
            }

            try {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(port)), TIMEOUT);

                DataOutputStream out = new DataOutputStream(socket.getOutputStream());

                out.writeUTF("insert");
                out.writeUTF(key);
                out.writeUTF(val);

                out.close();
                socket.close();

                Log.v("insert", port + " " + key + " " + val);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
    */

    @Override
    public boolean onCreate() {
        keyArray = new ArrayList<String>();
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
        new JoinTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            serverSocket.setReuseAddress(true);
            new ResponseTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        String key = selection;
        if (key==null) return null;

        Cursor cursor = null;
        if (key.equals("*")) {
            String[] columns = {KEY_FIELD, VALUE_FIELD};
            MatrixCursor mCursor = new MatrixCursor(columns);
            Iterator<String> it = keyArray.iterator();
            while (it.hasNext()) {
                String allKey = it.next();
                String allVal;
                File file = new File(getContext().getFilesDir().getAbsolutePath(), allKey);
                if (file.exists() && file.isFile()) {
                    try {
                        FileReader fReader = new FileReader(file);
                        BufferedReader bReader = new BufferedReader(fReader);
                        allVal = bReader.readLine();
                        bReader.close();
                        fReader.close();
                        mCursor.addRow(new Object[]{allKey, allVal});
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (!mSuccessor.equals(mID)) {
                try {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(mSuccessor))), TIMEOUT);

                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    DataInputStream in = new DataInputStream(socket.getInputStream());

                    out.writeUTF("queryAll");
                    out.writeUTF(mID);

                    String allKey = in.readUTF();
                    while (!allKey.equals("EOF")) {
                        String allVal = in.readUTF();
                        mCursor.addRow(new Object[]{allKey, allVal});
                        allKey = in.readUTF();
                    }

                    out.close();
                    in.close();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            cursor = mCursor;
        }
        else if (key.equals("@")) {
            String[] columns = {KEY_FIELD, VALUE_FIELD};
            MatrixCursor mCursor = new MatrixCursor(columns);
            Iterator<String> it = keyArray.iterator();
            while (it.hasNext()) {
                String atKey = it.next();
                String atVal;
                File file = new File(getContext().getFilesDir().getAbsolutePath(), atKey);
                if (file.exists() && file.isFile()) {
                    try {
                        FileReader fReader = new FileReader(file);
                        BufferedReader bReader = new BufferedReader(fReader);
                        atVal = bReader.readLine();
                        bReader.close();
                        fReader.close();
                        mCursor.addRow(new Object[]{atKey, atVal});
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            cursor = mCursor;
        }
        else {
            String[] columns = {KEY_FIELD, VALUE_FIELD};
            MatrixCursor mCursor = new MatrixCursor(columns);
            String hKey = "";
            try {
                hKey = genHash(key);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            String predecessor = find_predecessor(hKey);
            String successor = get_sucessor(predecessor);

            if (successor.equals(mID)) {
                File file = new File(getContext().getFilesDir().getAbsolutePath(), key);
                if (file.exists() && file.isFile()) {
                    String val;
                    try {
                        FileReader fReader = new FileReader(file);
                        BufferedReader bReader = new BufferedReader(fReader);
                        val = bReader.readLine();
                        bReader.close();
                        fReader.close();
                        mCursor.addRow(new Object[]{key, val});
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            else {
                try {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(successor))), TIMEOUT);

                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    DataInputStream in = new DataInputStream(socket.getInputStream());

                    out.writeUTF("query");
                    out.writeUTF(key);
                    String val = in.readUTF();

                    out.close();
                    in.close();
                    socket.close();

                    mCursor.addRow(new Object[]{key, val});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            cursor = mCursor;
        }
        Log.v("Query", selection);
        return cursor;
    }

    private class QueryTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... params) {
            return null;
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {

        return 0;
    }

    private static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class JoinTask extends AsyncTask<Void, Void, Void> {
        @Override
        protected Void doInBackground(Void... params) {
            boolean joined = false;

            for (int i=0; i<AVD_NO; i++) {
                if (myPort.equals(REMOTE_PORT[i])) continue;
                joined = join(REMOTE_PORT[i]);
                if (joined) break;
            }

            if (!joined) {
                join();
            }

            Log.v("Join", "ID="+mID+" Pred="+mPredecessor+" Succ="+mSuccessor);

            return null;
        }
    }

    private boolean join (String port){
        try {
            Socket socket = new Socket();

            //Log.v("Join", "Using " + port);

            socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(port)), TIMEOUT);

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            out.writeUTF("join");
            out.writeUTF(mID);
            mPredecessor = in.readUTF();
            //Log.v("Join", "mPredecessor=" + mPredecessor);
            mSuccessor = in.readUTF();

            out.close();
            in.close();
            socket.close();
            
            update_successor(mID, mPredecessor);
            update_predecessor(mID, mSuccessor);
            
        } catch (UnknownHostException e) {
            Log.e(TAG, "Socket UnknownHostException");
            return false;
        } catch (SocketTimeoutException e) {
            Log.e(TAG, "Socket Timeout " + port);
            return false;
        } catch (IOException e) {
            Log.e(TAG, "Socket IOException " + port);
            return false;
        } catch (Exception e) {
            Log.e(TAG, "Unknown Exception");
            return false;
        }

        return true;
    }

    private void update_successor(String n, String n_predecessor) {
        if (n.equals(n_predecessor)) return;

        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(n_predecessor))), TIMEOUT);

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            out.writeUTF("update_successor");
            out.writeUTF(n);

            out.close();
            in.close();
            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void update_predecessor(String n, String n_successor) {
        if (n.equals(n_successor)) return;

        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(n_successor))), TIMEOUT);

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            out.writeUTF("update_predecessor");
            out.writeUTF(n);

            String transKey = in.readUTF();
            while (!transKey.equals("EOF")) {
                String transVal = in.readUTF();

                keyArray.add(transKey);
                File file = new File(getContext().getFilesDir().getAbsolutePath(), transKey);
                if (file.exists() && file.isFile()) file.delete();
                try {
                    FileWriter fWrite = new FileWriter(file);
                    fWrite.write(transVal);
                    fWrite.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                Log.v("Transfer", "from "+portTable.get(n_successor)+" "+transKey+" "+transVal);

                transKey = in.readUTF();
            }

            out.close();
            in.close();
            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean join() {
        mPredecessor = mSuccessor = mID;
        return true;
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

    private String find_predecessor(String key) {
        if (isBetween(mID, key, mSuccessor)) {
            //Log.v("Between", "Return True");
            return mID;
        }
        else {
            String _predecessor = "";
            String port = portTable.get(mSuccessor);
            try {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(port)), TIMEOUT);

                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(socket.getInputStream());

                out.writeUTF("find_predecessor");
                out.writeUTF(key);
                _predecessor = in.readUTF();

                out.close();
                in.close();
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "Socket UnknownHostException");
            } catch (SocketTimeoutException e) {
                Log.e(TAG, "Socket timeout " + port);
            } catch (IOException e) {
                Log.e(TAG, "Socket IOException " + port);
            }
            return _predecessor;
        }
    }

    private String get_sucessor(String node) {
        if (node.equals(mID)) {
            return mSuccessor;
        }
        String port = portTable.get(node);
        String _successor = "";
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(port)), TIMEOUT);

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            out.writeUTF("get_sucessor");
            _successor = in.readUTF();

            out.close();
            in.close();
            socket.close();
        } catch (UnknownHostException e) {
            Log.e(TAG, "Socket UnknownHostException");
        } catch (SocketTimeoutException e) {
            Log.e(TAG, "Socket timeout " + port);
        } catch (IOException e) {
            Log.e(TAG, "Socket IOException " + port);
        }
        return _successor;
    }

    private class ResponseTask extends AsyncTask<ServerSocket, Void, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String request;

            while(true) {
                try {
                    //Log.v("ResponseTask", "Start Accept");
                    Socket server = serverSocket.accept();
                    //Log.v("ResponseTask", "Request Accepted");

                    DataOutputStream out = new DataOutputStream(server.getOutputStream());
                    DataInputStream in = new DataInputStream(server.getInputStream());

                    request = in.readUTF();

                    if (request.equals("join")) {
                        //Log.v("ResponseTask", "Join");

                        String key = in.readUTF();

                        //Log.v("ResponseTask", "Key="+key);

                        String _predecessor = find_predecessor(key);
                        //Log.v("ResponseTask", "find_predecessor "+_predecessor);
                        String _successor = get_sucessor(_predecessor);
                        //Log.v("ResponseTask", "get_sucessor "+_successor);
                        out.writeUTF(_predecessor);
                        out.writeUTF(_successor);
                    }
                    else if (request.equals("find_predecessor")) {
                        String key = in.readUTF();
                        String _predecessor = find_predecessor(key);
                        out.writeUTF(_predecessor);

                    }
                    else if (request.equals("get_sucessor")) {
                        out.writeUTF(mSuccessor);
                    }
                    else if (request.equals("update_successor")) {
                        mSuccessor = in.readUTF();
                    }
                    else if (request.equals("update_predecessor")) {
                        String prePredecessor = mPredecessor;
                        mPredecessor = in.readUTF();
                        Iterator<String> itKey = keyArray.iterator();
                        while (itKey.hasNext()) {
                            String transKey = itKey.next();
                            if (isBetween(prePredecessor, genHash(transKey), mPredecessor)) {
                                String transVal;
                                File file = new File(getContext().getFilesDir().getAbsolutePath(), transKey);
                                if (file.exists() && file.isFile()) {
                                    try {
                                        FileReader fReader = new FileReader(file);
                                        BufferedReader bReader = new BufferedReader(fReader);
                                        transVal = bReader.readLine();
                                        out.writeUTF(transKey);
                                        out.writeUTF(transVal);
                                        bReader.close();
                                        fReader.close();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                                file.delete();
                                itKey.remove();
                            }
                        }
                        out.writeUTF("EOF");
                    }
                    else if (request.equals("insert")) {
                        String key = in.readUTF();
                        String val = in.readUTF();
                        keyArray.add(key);
                        File file = new File(getContext().getFilesDir().getAbsolutePath(), key);
                        if (file.exists() && file.isFile()) file.delete();
                        try {
                            FileWriter fWrite = new FileWriter(file);
                            fWrite.write(val);
                            fWrite.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    else if (request.equals("query")) {
                        String key = in.readUTF();
                        File file = new File(getContext().getFilesDir().getAbsolutePath(), key);
                        if (file.exists() && file.isFile()) {
                            try {
                                FileReader fReader = new FileReader(file);
                                BufferedReader bReader = new BufferedReader(fReader);
                                String val = bReader.readLine();
                                out.writeUTF(val);
                                bReader.close();
                                fReader.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    else if (request.equals("queryAll")) {
                        String starter = in.readUTF();
                        ArrayList<String> cvArray = new ArrayList<String>();
                        Iterator<String> itKey = keyArray.iterator();
                        while (itKey.hasNext()) {
                            String allKey = itKey.next();
                            String allVal;
                            File file = new File(getContext().getFilesDir().getAbsolutePath(), allKey);
                            if (file.exists() && file.isFile()) {
                                try {
                                    FileReader fReader = new FileReader(file);
                                    BufferedReader bReader = new BufferedReader(fReader);
                                    allVal = bReader.readLine();
                                    bReader.close();
                                    fReader.close();
                                    cvArray.add(allKey);
                                    cvArray.add(allVal);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                        if (!mSuccessor.equals(starter)) {
                            try {
                                Socket socketQ = new Socket();
                                socketQ.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(mSuccessor))), TIMEOUT);

                                DataOutputStream outQ = new DataOutputStream(socketQ.getOutputStream());
                                DataInputStream inQ = new DataInputStream(socketQ.getInputStream());

                                outQ.writeUTF("queryAll");
                                outQ.writeUTF(starter);

                                String allKey = inQ.readUTF();
                                while (!allKey.equals("EOF")) {
                                    String allVal = inQ.readUTF();
                                    cvArray.add(allKey);
                                    cvArray.add(allVal);
                                    allKey = inQ.readUTF();
                                }

                                outQ.close();
                                inQ.close();
                                socketQ.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        Iterator<String> itCV = cvArray.iterator();
                        while (itCV.hasNext()) {
                            out.writeUTF(itCV.next());
                            out.writeUTF(itCV.next());
                        }
                        out.writeUTF("EOF");
                    }
                    else if (request.equals("deleteAll")) {
                        String starter = in.readUTF();
                        Iterator<String> it = keyArray.iterator();
                        while (it.hasNext()) {
                            String atKey = it.next();
                            File file = new File(getContext().getFilesDir().getAbsolutePath(), atKey);
                            file.delete();
                            it.remove();
                        }
                        if (!mSuccessor.equals(starter)) {
                            try {
                                Socket socketD = new Socket();
                                socketD.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portTable.get(mSuccessor))), TIMEOUT);

                                DataOutputStream outD = new DataOutputStream(socketD.getOutputStream());

                                outD.writeUTF("deleteAll");
                                outD.writeUTF(mID);

                                outD.close();
                                socketD.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    out.close();
                    in.close();
                    server.close();

                    //Log.v("Update", "ID=" + mID + " Pred=" + mPredecessor + " Succ=" + mSuccessor);

                } catch (IOException e) {
                    Log.e(TAG, "Can't receive message");
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }

            //return null;
        }
    }
}
