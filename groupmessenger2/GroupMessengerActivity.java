package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */

/*
message format:
regular message: {message type, sender port number, sender sequence number, message string}
priority message: {message type, proposed/agreed, sender port number, sender sequence number, priority}
 */

public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String [] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final int AVD_NO = 5;
    private static final int BUF_LEN = 100;
    private static final int TIMEOUT = 10000;
    private static final int DETECT_INTERVAL = 10000;

    private EditText et;
    private TextView tv;
    private Button bt;
    private String myPort;

    private Uri mUri;

    private class SeqVector {
        Integer [] sv;
        public SeqVector() {
            sv = new Integer[]{0,0,0,0,0};
        }
        public void addOne(String port) {
            sv[(Integer.parseInt(port)-11108)/4]++;
        }
        public Integer getSeq(String port) {
            return sv[(Integer.parseInt(port)-11108)/4];
        }
    }

    //private SeqVector seqVector;

    private Integer [] delivSeqVector;
    private Integer sndSeq;
    private Boolean [] isAlive;
    private Integer delivSeqNo;
    private Integer propPriority;

    static class PriCompare implements Comparator<PriorityCell> {

        @Override
        public int compare(PriorityCell one, PriorityCell two) {
            if (one.bufferNO==two.bufferNO) {
                return one.seqNO - two.seqNO;
            }
            else if (one.priority!=two.priority) {
                return one.priority - two.priority;
            }
            else {
                return one.bufferNO - two.bufferNO;
            }
        }
    }

    public enum MsgType {REG_MSG, PRI_MSG}
    public enum PriType {PROPOSED, AGREED}

    public class BufferCell {
        ArrayList<String> msgArray;
        Integer [] propPri;
        boolean agreeSent;
        public BufferCell() {
            msgArray = null;
            agreeSent = false;
            propPri = new Integer[]{0, 0, 0, 0, 0};
        }
    }

    public ArrayList<ArrayList<BufferCell>> buffers;

    public class PriorityCell {
        Integer bufferNO;
        Integer seqNO;
        Integer priority;
        boolean t_deliverable;
        String msg;
    }

    PriorityQueue<PriorityCell> priQueue;

    @Override
    protected void onCreate(Bundle savedInstanceState) {

        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        isAlive = new Boolean[]{true, true, true, true, true};
        //seqVector = new SeqVector();
        delivSeqNo = new Integer(0);
        propPriority = new Integer(0);
        sndSeq = new Integer(0);
        delivSeqVector = new Integer[]{0,0,0,0,0};

        buffers = new ArrayList<ArrayList<BufferCell>>(AVD_NO); // 5 buffers
        for (int i=0; i<AVD_NO; i++) {
            ArrayList<BufferCell> tempAL = new ArrayList<BufferCell>(BUF_LEN);
            for (int j=0; j<BUF_LEN; j++) tempAL.add(null);
            buffers.add(tempAL);
        }

        PriCompare comparator = new PriCompare();
        priQueue = new PriorityQueue<PriorityCell>(BUF_LEN, comparator); // priority queue


        mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        //Log.v("myPort", myPort.toString());

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            serverSocket.setReuseAddress(true);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        et = (EditText) findViewById(R.id.editText1);
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        bt = (Button) findViewById(R.id.button4);
        bt.setOnClickListener(new SendOnClickListener());

        //new FailureDetector().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
        FailureDetector fd = new FailureDetector();
        new Thread(fd).start();

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }


    public class SendOnClickListener implements View.OnClickListener {
        public void onClick (View v) {
            String msg = et.getText().toString() + "\n";
            et.setText("");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, ArrayList<String>, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            //Log.v("server", "server socket");
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

            //long startTime=new Date().getTime();
            //while((new Date().getTime())-startTime<100000) {

            while(true) {
                try {
                    Socket server = serverSocket.accept();

                    ObjectInputStream ois = new ObjectInputStream(server.getInputStream());
                    ArrayList<String> recMsgArray = (ArrayList<String>) ois.readObject();
                    //publishProgress(recMsgArray.get(0)+" "+recMsgArray.get(1)+" "+recMsgArray.get(2)+" "+recMsgArray.get(3));

                    publishProgress(recMsgArray);

                } catch (IOException e) {
                    Log.e(TAG, "Can't receive message");
                } catch (ClassNotFoundException e) {
                    Log.e(TAG, "Can't find class");
                }
            }

            //return null;
        }

        protected void onProgressUpdate(ArrayList<String>...als) {
            /*
             * The following code displays what is received in doInBackground().
             */

            ArrayList<String> recMsgArray = als[0];

            if (recMsgArray.get(0).equals("hello")) {
                //tv.append(recMsgArray.get(0) + "\t\n");
                return;
            }

            String strReceived = recMsgArray.get(0)+" "+recMsgArray.get(1)+" "+recMsgArray.get(2)+" "+recMsgArray.get(3)+" "+recMsgArray.get(4)+" "+recMsgArray.get(5);
            tv.append(strReceived + "\t\n");

            String msgType_s = recMsgArray.get(0);
            // When receiving a regular message, buffer it according to its sequence number, put it into the priority queue,
            // and reply the proposed priority to the sender.
            if (msgType_s.equals(MsgType.REG_MSG.toString())) {

                String portNO = recMsgArray.get(1);
                Integer portIndex = (Integer.parseInt(portNO)-11108)/4;
                Integer seqNO = Integer.parseInt(recMsgArray.get(2));
                String msg = recMsgArray.get(3);

                Log.v("REG_MSG", portIndex.toString()+" "+seqNO.toString());

                BufferCell bufferCell = new BufferCell();
                bufferCell.msgArray = recMsgArray;

                buffers.get(portIndex).set(seqNO % BUF_LEN, bufferCell); // to act as a circular queue

                //Log.v("add buffer cell", portIndex.toString()+" "+seqNO.toString());

                PriorityCell priCell = new PriorityCell();
                propPriority++;
                //priCell.priority = propPriority * AVD_NO + portIndex; // to express the value of propPriority.portIndex
                priCell.priority = propPriority;
                priCell.bufferNO = portIndex;
                priCell.seqNO = seqNO;
                priCell.t_deliverable = false;
                priCell.msg = msg;
                priQueue.add(priCell);

                ////////////////////////////////////////////////////////////////////////////////////
                Iterator<PriorityCell> it = priQueue.iterator();
                String q = new String();
                while (it.hasNext()) {
                    PriorityCell pc = it.next();
                    String m = " ["+pc.bufferNO.toString()+" "+pc.seqNO.toString()+" "+pc.priority.toString()+" "+String.valueOf(pc.t_deliverable)+"] ";
                    q += m;
                }
                Log.v("Priority Queue", q);
                ////////////////////////////////////////////////////////////////////////////////////

                ArrayList<String> replyMsgArray = new ArrayList<String>();
                replyMsgArray.add(MsgType.PRI_MSG.toString());    // message type
                replyMsgArray.add(PriType.PROPOSED.toString());   // proposed or agreed
                replyMsgArray.add(portNO);                        // port number
                replyMsgArray.add(seqNO.toString());              // sequence number
                //replyMsgArray.add(Integer.toString(propPriority * AVD_NO + portIndex)); // priority
                replyMsgArray.add(Integer.toString(propPriority));
                replyMsgArray.add(myPort);

                new ReplyTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, replyMsgArray);

            }
            // When receiving the priority message
            else if (msgType_s.equals(MsgType.PRI_MSG.toString())) {

                String priType_s = recMsgArray.get(1);
                String portNO = recMsgArray.get(2);
                Integer portIndex = (Integer.parseInt(portNO)-11108)/4;
                Integer seqNO = Integer.parseInt(recMsgArray.get(3));
                Integer priority = Integer.parseInt(recMsgArray.get(4));

                // If this is a proposed message, update the proposed priority, check if all alive AVDs have proposed
                if (priType_s.equals(PriType.PROPOSED.toString())) {
                    assert(portNO.equals(myPort));
                    String fromPortNO = recMsgArray.get(5);
                    Integer fromPortIndex = (Integer.parseInt(fromPortNO)-11108)/4;

                    Log.v("PROPOSE_MSG", portIndex.toString()+" "+seqNO.toString()+" from "+fromPortIndex.toString());

                    //Log.v("get buffered priority", portIndex.toString()+" "+seqNO.toString()+" "+fromPortIndex.toString());
                    //Log.v("get buffered priority", String.valueOf(buffers.get(portIndex).get(seqNO)));

                    buffers.get(portIndex).get(seqNO).propPri[fromPortIndex] = priority;
                    Integer agreedPri = Integer.parseInt(recMsgArray.get(4));
                    boolean allProposed = true;
                    for (int i=0; i<AVD_NO; i++) {
                        if (isAlive[i]) {
                            Integer tempPri = buffers.get(portIndex).get(seqNO).propPri[i];
                            //Log.v("tempPri", portIndex.toString()+" "+seqNO.toString()+" "+String.valueOf(i)+" "+tempPri.toString());

                            if (tempPri!=0) {
                                agreedPri = tempPri>agreedPri? tempPri:agreedPri;
                            }
                            else {
                                allProposed = false;
                                break;
                            }
                        }
                    }

                    // If all AVDs have proposed, update propPriority, and send priority message to all AVDs
                    if (allProposed) {
                        propPriority = propPriority>=agreedPri? propPriority:agreedPri;

                        ArrayList<String> agreeMsgArray = new ArrayList<String>();
                        agreeMsgArray.add(MsgType.PRI_MSG.toString());    // message type
                        agreeMsgArray.add(PriType.AGREED.toString());   // proposed or agreed
                        agreeMsgArray.add(portNO);                        // port number
                        agreeMsgArray.add(seqNO.toString());              // sequence number
                        agreeMsgArray.add(Integer.toString(agreedPri)); // priority

                        agreeMsgArray.add(null);

                        new AgreeTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, agreeMsgArray);
                    }
                }
                // If this is an agreed message, update priority in priQueue, and deliver appropriate message(s)
                else if (priType_s.equals(PriType.AGREED.toString())) {

                    Log.v("AGREE_MSG", portIndex.toString()+" "+seqNO.toString());

                    PriorityCell n_priCell = new PriorityCell();
                    n_priCell.bufferNO = portIndex;
                    n_priCell.seqNO = seqNO;
                    n_priCell.priority = priority;
                    n_priCell.t_deliverable = true;

                    Iterator<PriorityCell> it = priQueue.iterator();
                    while (it.hasNext()) {
                        PriorityCell pc = it.next();
                        if (pc.bufferNO==n_priCell.bufferNO && pc.seqNO==n_priCell.seqNO) {
                            n_priCell.msg = pc.msg;
                            priQueue.remove(pc);
                            break;
                        }
                    }
                    priQueue.add(n_priCell);

                    ////////////////////////////////////////////////////////////////////////////////
                    Iterator<PriorityCell> itt = priQueue.iterator();
                    String q = new String();
                    while (itt.hasNext()) {
                        PriorityCell pc = itt.next();
                        String m = " ["+pc.bufferNO.toString()+" "+pc.seqNO.toString()+" "+pc.priority.toString()+" "+String.valueOf(pc.t_deliverable)+"] ";
                        q += m;
                    }
                    Log.v("Priority Queue", q);
                    ////////////////////////////////////////////////////////////////////////////////

                    // deliver
                    PriorityCell pc = priQueue.peek();

                    //Log.v("deliver", pc.bufferNO.toString()+" "+pc.seqNO.toString()+" "+pc.msg+" "+String.valueOf(pc.t_deliverable));

                    while (pc!=null && pc.t_deliverable) {
                        if (delivSeqVector[pc.bufferNO]+1==pc.seqNO) {
                            String key = delivSeqNo.toString();
                            delivSeqNo++;
                            ContentValues cv = new ContentValues();
                            cv.put(KEY_FIELD, key);
                            cv.put(VALUE_FIELD, pc.msg);
                            getContentResolver().insert(mUri, cv);

                            delivSeqVector[pc.bufferNO] = (delivSeqVector[pc.bufferNO]+1)%BUF_LEN;

                            buffers.get(pc.bufferNO).set(pc.seqNO, null);
                            priQueue.poll();
                            pc = priQueue.peek();
                        }
                    }
                }

            }

            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            Socket socket;
            //DataOutputStream out;

            ArrayList<String> msgArray = new ArrayList<String>();

            msgArray.add(MsgType.REG_MSG.toString());   // message type
            msgArray.add(myPort);                       // port
            //Integer sndSeq = seqVector.getSeq(myPort)+1;
            //seqVector.addOne(myPort);
            sndSeq++;
            msgArray.add(sndSeq.toString());            // sending sequence number
            //msgArray.add(Boolean.toString(false));      // total_deliverable
            msgArray.add(msgs[0].trim());                      // message

            msgArray.add(null); //
            msgArray.add(null);

            // send to itself first
            try {
                socket = new Socket();
                socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(myPort)), TIMEOUT);
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(msgArray);

                oos.close();
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (SocketTimeoutException e) {
                //isAlive[(Integer.parseInt(myPort)-11108)/4] = false;
                FailureHandling(msgArray, myPort);
                //FailureHandling1(msgArray, myPort);
                Log.e("Self timeout", myPort);
            } catch (IOException e) {
                FailureHandling(msgArray, myPort);
                //FailureHandling1(msgArray, myPort);
                Log.e(TAG, "ClientTask socket IOException "+myPort.toString());
            }

            // then send to others
            for (int i=0; i<AVD_NO; i++) {

                if (!isAlive[i]) continue;
                if (REMOTE_PORT[i].equals(myPort)) continue;

                try {
                    socket = new Socket();
                    socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(REMOTE_PORT[i])), TIMEOUT);
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(msgArray);
                    oos.close();
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (SocketTimeoutException e) {
                    //isAlive[i] = false;
                    FailureHandling(msgArray, REMOTE_PORT[i]);
                    //FailureHandling1(msgArray, REMOTE_PORT[i]);
                    Log.e("Client timeout", REMOTE_PORT[i]);
                } catch (IOException e) {
                    FailureHandling(msgArray, REMOTE_PORT[i]);
                    //FailureHandling1(msgArray, REMOTE_PORT[i]);
                    Log.e(TAG, "ClientTask socket IOException "+REMOTE_PORT[i]);
                }
            }
            return null;
        }
    }

    private class ReplyTask extends AsyncTask<ArrayList<String>, Void, Void> {
        @Override
        protected Void doInBackground(ArrayList<String>... als) {

            ArrayList<String> replyMsgArray = als[0];

            String portNO = replyMsgArray.get(2);
            Integer portIndex = (Integer.parseInt(portNO)-11108)/4;

            if (!isAlive[portIndex]) return null;

            try {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portNO)), TIMEOUT);
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(replyMsgArray);
                oos.close();
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (SocketTimeoutException e) {
                //isAlive[portIndex] = false;
                FailureHandling(replyMsgArray, portNO);
                //FailureHandling1(replyMsgArray, portNO);
                Log.e("Reply timeout", portNO);
            } catch (IOException e) {
                FailureHandling(replyMsgArray, portNO);
                //FailureHandling1(replyMsgArray, portNO);
                Log.e(TAG, "ReplyTask socket IOException "+portNO);
            }
            return null;
        }
    }

    private class AgreeTask extends AsyncTask<ArrayList<String>, Void, Void> {

        @Override
        protected Void doInBackground(ArrayList<String>... als) {

            ArrayList<String> agreeMsgArray = als[0];
            Integer myIndex = (Integer.parseInt(myPort)-11108)/4;
            Integer seqNO = Integer.parseInt(agreeMsgArray.get(3));
            buffers.get(myIndex).get(seqNO).agreeSent = true;

            for (int i=0; i<AVD_NO; i++) {
                if (!isAlive[i]) continue;
                try {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(REMOTE_PORT[i])), TIMEOUT);
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(agreeMsgArray);
                    oos.close();
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (SocketTimeoutException e) {
                    //isAlive[i] = false;
                    FailureHandling(agreeMsgArray, REMOTE_PORT[i]);
                    //FailureHandling1(agreeMsgArray, REMOTE_PORT[i]);
                    Log.e("Agree timeout", REMOTE_PORT[i]);
                } catch (IOException e) {
                    FailureHandling(agreeMsgArray, REMOTE_PORT[i]);
                    //FailureHandling1(agreeMsgArray, REMOTE_PORT[i]);
                    Log.e(TAG, "AgreeTask socket IOException "+REMOTE_PORT[i]);
                }
            }

            return null;
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private void FailureHandling(ArrayList<String> sndMsg, String sndPort) {

        isAlive[(Integer.parseInt(sndPort)-11108)/4] = false;

        Log.v("Priority Queue", "start removing...");

        Iterator<PriorityCell> it = priQueue.iterator();
        while (it.hasNext()) {
            PriorityCell pc = it.next();
            if (!pc.t_deliverable && !isAlive[pc.bufferNO]) {
                Integer bufferIndex = pc.bufferNO;
                Integer seqIndex = pc.seqNO;
                buffers.get(bufferIndex).set(seqIndex, null);
                priQueue.remove(pc);
            }
        }

        ////////////////////////////////////////////////////////////////////////////////
        Iterator<PriorityCell> itt = priQueue.iterator();
        String q = new String();
        while (itt.hasNext()) {
            PriorityCell pcc = itt.next();
            String m = " ["+pcc.bufferNO.toString()+" "+pcc.seqNO.toString()+" "+pcc.priority.toString()+" "+String.valueOf(pcc.t_deliverable)+"] ";
            q += m;
        }
        Log.v("Priority Queue", q);
        ////////////////////////////////////////////////////////////////////////////////

        PriorityCell pc = priQueue.peek();

        //Log.v("why not here", "1");

        while (pc!=null && pc.t_deliverable) {
            if (delivSeqVector[pc.bufferNO]+1==pc.seqNO) {
                String key = delivSeqNo.toString();
                delivSeqNo++;
                ContentValues cv = new ContentValues();
                cv.put(KEY_FIELD, key);
                cv.put(VALUE_FIELD, pc.msg);
                getContentResolver().insert(mUri, cv);

                delivSeqVector[pc.bufferNO] = (delivSeqVector[pc.bufferNO]+1)%BUF_LEN;

                buffers.get(pc.bufferNO).set(pc.seqNO, null);
                priQueue.poll();
                pc = priQueue.peek();
            }
        }

        //Log.v("why not here", "2");

        Integer myIndex = (Integer.parseInt(myPort)-11108)/4;

        Log.v("failure handling", "check starting from "+myIndex.toString()+" "+String.valueOf(delivSeqVector[myIndex] + 1));

        for (int j = delivSeqVector[myIndex] + 1; j != delivSeqVector[myIndex]; j = (j + 1) % BUF_LEN) {

            //Log.v("failure handling", "check "+myIndex.toString()+" "+String.valueOf(j));

            if (buffers.get(myIndex).get(j) == null) {
                //Log.v("failure handling", myIndex.toString()+" "+String.valueOf(j)+" = null");
                continue;
            }
            if (buffers.get(myIndex).get(j).agreeSent) {
                //Log.v("failure handling", myIndex.toString()+" "+String.valueOf(j)+".agreeSent = true");
                continue;
            }

            boolean allProposed = true;
            Integer agreedPri = -1;
            for (int k = 0; k < AVD_NO; k++) {
                if (!isAlive[k]) continue;
                Integer tempPri = buffers.get(myIndex).get(j).propPri[k];
                if (tempPri != 0) {
                    agreedPri = tempPri > agreedPri ? tempPri : agreedPri;
                } else {
                    allProposed = false;
                    break;
                }
            }
            if (allProposed && agreedPri != -1) {

                Log.v("failure handling", "send agree "+myIndex.toString()+" "+String.valueOf(j));

                propPriority = propPriority >= agreedPri ? propPriority : agreedPri;

                ArrayList<String> agreeMsgArray = new ArrayList<String>();
                agreeMsgArray.add(MsgType.PRI_MSG.toString());    // message type
                agreeMsgArray.add(PriType.AGREED.toString());   // proposed or agreed
                agreeMsgArray.add(myPort);                      // port number
                agreeMsgArray.add(String.valueOf(j));              // sequence number
                agreeMsgArray.add(Integer.toString(agreedPri)); // priority

                agreeMsgArray.add(null);

                buffers.get(myIndex).get(j).agreeSent = true;
                for (int i=0; i<AVD_NO; i++) {
                    if (!isAlive[i]) continue;
                    try {
                        Socket socket = new Socket();
                        socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(REMOTE_PORT[i])), TIMEOUT);
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(agreeMsgArray);
                        oos.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (SocketTimeoutException e) {
                        //isAlive[i] = false;
                        FailureHandling(agreeMsgArray, REMOTE_PORT[i]);
                        Log.e("FailureHandling timeout", REMOTE_PORT[i]);
                    } catch (IOException e) {
                        FailureHandling(agreeMsgArray, REMOTE_PORT[i]);
                        Log.e(TAG, "FailureHandling socket IOException "+REMOTE_PORT[i]);
                    }
                }
            }
        }

        // if fails, then update isAlive, check if agreed message is needed
        // if not fails, re-send the message, for a given amount of times
    }

    private class FailureDetector1 extends AsyncTask<Void, ArrayList<String>, Void> {

        @Override
        protected Void doInBackground(Void... params) {

            ArrayList<String> hello = new ArrayList<String>();
            hello.add("hello");

            while (true) {

                Log.v("failure detection", "loop start");

                boolean failureDetected = false;
                for (int i = 0; i < AVD_NO; i++) {

                    if (!isAlive[i]) continue;
                    //if (REMOTE_PORT[i].equals(myPort)) continue;

                    int counter = 0;
                    for (int j = 0; j < 1; j++) {
                        try {
                            Socket socket = new Socket();
                            socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(REMOTE_PORT[i])), 50);
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            oos.writeObject(hello);
                            oos.close();
                            socket.close();
                        } catch (UnknownHostException e) {
                            Log.e(TAG, "ClientTask UnknownHostException");
                        } catch (SocketTimeoutException e) {
                            counter++;
                        } catch (IOException e) {
                            counter++;
                        }
                    }
                    if (counter >= 1) {
                        isAlive[i] = false;
                        failureDetected = true;

                        Log.v("failure detection", "AVD "+String.valueOf(i));
                    }
                }

                if (failureDetected) {

                    Log.v("FailureDetector", "failure detected");

                    for (int i = 0; i < AVD_NO; i++) {
                        if (!isAlive[i]) continue;

                        for (int j = delivSeqVector[i] + 1; j < delivSeqVector[i]; j = (j + 1) % BUF_LEN) {
                            if (buffers.get(i).get(j) == null) continue;

                            boolean allProposed = true;
                            Integer agreedPri = -1;
                            for (int k = 0; k < AVD_NO; k++) {
                                if (!isAlive[k]) continue;
                                Integer tempPri = buffers.get(i).get(j).propPri[k];
                                if (tempPri != 0) {
                                    agreedPri = tempPri > agreedPri ? tempPri : agreedPri;
                                } else {
                                    allProposed = false;
                                    break;
                                }
                            }
                            if (allProposed && agreedPri != -1) {
                                propPriority = propPriority >= agreedPri ? propPriority : agreedPri;

                                ArrayList<String> agreeMsgArray = new ArrayList<String>();
                                agreeMsgArray.add(MsgType.PRI_MSG.toString());    // message type
                                agreeMsgArray.add(PriType.AGREED.toString());   // proposed or agreed
                                agreeMsgArray.add(String.valueOf(i * 4 + 11108));   // port number
                                agreeMsgArray.add(String.valueOf(j));              // sequence number
                                agreeMsgArray.add(Integer.toString(agreedPri)); // priority

                                agreeMsgArray.add(null);

                                publishProgress(agreeMsgArray);

                            }
                        }

                    }
                }

                Log.v("failure detection", "loop end");
            }

            //return null;
        }

        protected void onProgressUpdate(ArrayList<String>...als) {

            ArrayList<String> agreeMsgArray = als[0];
            new AgreeTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, agreeMsgArray);
        }
    }

    class FailureDetector implements Runnable {

        public void run() {

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            ArrayList<String> hello = new ArrayList<String>();
            hello.add("hello");

            //int count = 0;

            while (true) {

                //Log.v("failure detection", String.valueOf(count++));

                for (int i = 0; i < AVD_NO; i++) {

                    boolean failureDetected = false;
                    if (!isAlive[i]) continue;
                    //if (REMOTE_PORT[i].equals(myPort)) continue;

                    try {
                        Socket socket = new Socket();
                        socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(REMOTE_PORT[i])), TIMEOUT);
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(hello);
                        oos.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (SocketTimeoutException e) {
                        //isAlive[i] = false;
                        failureDetected = true;
                    } catch (IOException e) {
                        //isAlive[i] = false;
                        failureDetected = true;
                    }

                    if (failureDetected) {

                        Log.v("FailureDetector", "failure detected");

                        FailureHandling(hello, REMOTE_PORT[i]);
                    }
                }

                try {
                    Thread.sleep(DETECT_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                //Log.v("failure detection", "loop end");
            }

            //return null;
        }
    }

    private void FailureHandling11(ArrayList<String> sndMsg, String sndPort) {

        if (!isAlive[(Integer.parseInt(sndPort)-11108)/4]) return;

        // re-send the message
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(sndPort)), TIMEOUT);
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(sndMsg);
            oos.close();
            socket.close();
        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (SocketTimeoutException e) {
            //isAlive[i] = false;
            //FailureHandling(agreeMsgArray, REMOTE_PORT[i]);
            FailureHandling11(sndMsg, sndPort);
            Log.e("Agree timeout", sndPort);
        } catch (IOException e) {
            //FailureHandling(agreeMsgArray, REMOTE_PORT[i]);
            FailureHandling11(sndMsg, sndPort);
            Log.e(TAG, "ClientTask socket IOException");
        }
    }
}
