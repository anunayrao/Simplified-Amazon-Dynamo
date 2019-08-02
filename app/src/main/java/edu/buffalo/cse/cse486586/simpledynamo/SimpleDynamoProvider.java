package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static java.lang.Thread.sleep;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	public static Object Lock=new Object();
	boolean recoveryCompleted;
	static final int SERVER_PORT = 10000;
	static String myavdPort="";    //5554 Format
	String predecessor="";         //5554 Format
	String successor="";            //5554 Format
	ArrayList<String> remotePort = new ArrayList<String>();
	ArrayList<Node> ring = new ArrayList<Node>();
	ArrayList<String> Files = new ArrayList<String>();
	BlockingQueue<String> AllKeyVal = new ArrayBlockingQueue<String>(1);
	BlockingQueue<String> QueryKeyVal = new ArrayBlockingQueue<String>(1);
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		if(selection.contains("@")){
			//https://stackoverflow.com/questions/3554722/how-to-delete-internal-storage-file-in-android
			File dir[] = getContext().getFilesDir().listFiles();
			for( File file : dir){
				file.delete();
			}
		}
		else if(selection.contains("*")){
			File dir[] = getContext().getFilesDir().listFiles();
			for( File file : dir){
				file.delete();
			}
			String msg= "Deleteall";
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg,successor);
			Log.d(TAG, "Deleted all Successfully");
		}
		else{

			File dir = getContext().getFilesDir();
			File file = new File(dir, selection);
			if(file.exists()){
				file.delete();
			}
			String msg = "Delete:"+selection;
			if(selectionArgs==null) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myavdPort);
			}

		}
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		Log.e("MYAVDPORT::", myavdPort);
		String key = values.getAsString("key");
		String value = values.getAsString("value");
		String tvalue;

		if(!value.contains("-")){
			Log.e("INSERT","new:"+key);
			tvalue = value + "-" + System.currentTimeMillis();
			try{
				Node first = ring.get(0);
				Node last = ring.get(4);
				if ((genHash(key).compareTo(first.node_id) < 0) || (genHash(key).compareTo(last.node_id) > 0)) {
					String msg = "Insert:" + key + ":" + tvalue;
					Log.e("INSERT::","SEnding to Node 0");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg,first.port);

				}
				else{
					for (int i = 0; i < ring.size() - 1; i++) {
						Node node = ring.get(i);
						Node nextNode = ring.get(i + 1);
						if (genHash(key).compareTo(node.node_id) > 0 && genHash(key).compareTo(nextNode.node_id) < 0) {
							String msg = "Insert:" + key + ":" + tvalue;
							Log.e("INSERT::","SEnding to Node"+nextNode.port);
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, nextNode.port);


						}
					}
				}

			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		else{
			tvalue = value;
			Log.e("INSERT","in me :"+key);
			Context con = getContext();
			File dir = getContext().getFilesDir();
			File file = new File(dir, key);
			if(file.exists()){

				FileInputStream fis = null;
				try {
					fis = con.openFileInput(key);
					BufferedReader br = new BufferedReader(new InputStreamReader(fis));
					String record = br.readLine();
					Long eval = Long.parseLong(record.split("-")[1]);
					Long nval = Long.parseLong(tvalue.split("-")[1]);
					if(nval>eval){
						OutputStreamWriter osw = new OutputStreamWriter(getContext().openFileOutput(key, Context.MODE_PRIVATE));
						osw.write(tvalue);
						osw.close();
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
			else {
				Files.add(key);
				try {
					OutputStreamWriter osw = new OutputStreamWriter(getContext().openFileOutput(key, Context.MODE_PRIVATE));
					osw.write(tvalue);
					osw.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		Log.d("INSERTED::", key + ":" + tvalue);
		Log.d("FILE SIZE", "" + Files.size());
		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		remotePort.add(REMOTE_PORT0);
		remotePort.add(REMOTE_PORT1);
		remotePort.add(REMOTE_PORT2);
		remotePort.add(REMOTE_PORT3);
		remotePort.add(REMOTE_PORT4);
		String myPortID = getMyPort();
		//11108:5554
		String myPort = myPortID.split(":")[0];
		String myID = myPortID.split(":")[1];
		String port = myID;   //5554 Format
		myavdPort = myID;  //5554 Format
		recoveryCompleted = false;
		try {
			ServerSocket  serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't Create Server Socket");
			e.printStackTrace();
		}

		try {
			Node n1 = new Node(genHash("5562"),null,null,"5562","11124");
			Node n2 = new Node(genHash("5556"),n1,null,"5556","11112" );
			Node n3 = new Node(genHash("5554"),n2,null,"5554","11108");
			Node n4 = new Node(genHash("5558"),n3,null,"5558","11116");
			Node n5 = new Node(genHash("5560"),n4,n1,"5560","11120");
			n1.setSuccessor(n2);
			n1.setPredecessor(n5);
			n2.setSuccessor(n3);
			n2.setPredecessor(n1);
			n3.setSuccessor(n4);
			n3.setPredecessor(n2);
			n4.setSuccessor(n5);
			n4.setPredecessor(n3);
			n5.setSuccessor(n1);
			n5.setPredecessor(n4);
			ring.add(n1);
			ring.add(n2);
			ring.add(n3);
			ring.add(n4);
			ring.add(n5);
			Log.d(TAG, "Dynamo ring is ready!!");

			if(myPort.equals(REMOTE_PORT0)){
				predecessor = "5556";
				successor="5558";
			}
			else if(myPort.equals(REMOTE_PORT1)){
				predecessor="5562";
				successor="5554";
			}
			else if(myPort.equals(REMOTE_PORT2)){
				predecessor="5554";
				successor="5560";
			}
			else if(myPort.equals(REMOTE_PORT3)){
				predecessor="5558";
				successor="5562";
			} else if (myPort.equals(REMOTE_PORT4)) {
				predecessor="5560";
				successor="5556";
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		//Ring Order is Correct for sure.
		//getReplicaNodes is working perfectly
		//getCoordinator is working perfectly




		String msg = "Recovery";
		// On Recovery onCreate will be called handle it here.
		delete(mUri,"@",null);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg,myavdPort);

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});
		Context con = getContext();
		String data;
		Log.d(TAG, "QueryMethod: Selection:" + selection);
		if(sortOrder!=null){
		if(selection.contains("@") && sortOrder.equals("recover")){
			for (int i = 0; i < Files.size(); i++) {
				try {
					FileInputStream fis = con.openFileInput(Files.get(i));
					BufferedReader br = new BufferedReader(new InputStreamReader(fis));
					while ((data = br.readLine()) != null) {
						mc.addRow(new String[]{Files.get(i), data});

					}
					fis.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return mc;
		}}
		else if (selection.contains("@")) {
			while(!recoveryCompleted){
				try {
					sleep(20);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			for (int i = 0; i < Files.size(); i++) {
				try {
					FileInputStream fis = con.openFileInput(Files.get(i));
					BufferedReader br = new BufferedReader(new InputStreamReader(fis));
					while ((data = br.readLine()) != null) {
						mc.addRow(new String[]{Files.get(i), data.split("-")[0]});

					}
					fis.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return mc;
		}
		else if (selection.contains("*")) {
			Log.d(TAG, "QueryMethod: Type *");
			for (int i = 0; i < Files.size(); i++) {
				try {
					FileInputStream fis = con.openFileInput(Files.get(i));
					BufferedReader br = new BufferedReader(new InputStreamReader(fis));
					while ((data = br.readLine()) != null) {
						mc.addRow(new String[]{Files.get(i), data.split("-")[0]});

					}
					fis.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			String msg = "ALLKVPAIRS*";

				Log.d(TAG, "Calling Client from QueryMethod to Give all key value ");
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myavdPort);

				// Collect Key Value Pairs from other nodes and add it to matrix cursor.

				String str = null;
				try {
					str = AllKeyVal.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				String kvps[] = str.split("\\|");
				for (int i = 0; i < kvps.length; i++) {
					Log.d(TAG, "KVPS:" + i + ":" + kvps[i]);
					if (kvps[i] != "") {
						String k = kvps[i].split(":")[0];
						String v = kvps[i].split(":")[1];
						mc.addRow(new String[]{k, v.split("-")[0]});
					}
				}
				AllKeyVal.clear();

			return mc;

		}else {
			Node first = null;
			Node last = null;
			try {
				first = ring.get(0);
				last = ring.get(4);
				if (genHash(selection).compareTo(first.node_id) < 0 || genHash(selection).compareTo(last.node_id) > 0) {
					//From Node 0
					String msg = "Query:" + selection;
				String str = 	new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, first.port).get();
					Log.d(TAG, "QueryMethod:" + "Blocking Array Waiting");
					/*
					String str = null;
					try {
						str = QueryKeyVal.take();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					*/

					//Here get all the three values compare their timestamp and then add it to cursor and return

					String crkv[] = str.split("\\|");

					ArrayList<String> keys = new ArrayList<String>();
					ArrayList<String> values = new ArrayList<String>();
					ArrayList<Long> time = new ArrayList<Long>();
					for(int i=0; i<crkv.length;i++){
						if(!crkv[i].equals("null")&& !crkv[i].equals("")&& !crkv[i].equals(null)) {
							keys.add(crkv[i].split(":")[0]);
							values.add(crkv[i].split(":")[1].split("-")[0]);
							time.add(Long.parseLong(crkv[i].split(":")[1].split("-")[1]));
						}
					}

					//Find maximum time value index
					int i = time.indexOf(Collections.max(time));

					Log.d(TAG, "QueryMethod:" + "Blocking Array Got Value");
					String k = keys.get(i);
					String v = values.get(i);
					Log.d("RETURN FINAL VALUE",""+k+":"+v);
					mc.addRow(new String[]{k, v});

					//QueryKeyVal.clear();
					return mc;
					//Bring from Node 0 and its replicas
				} else {
					for (int i = 0; i < ring.size() - 1; i++) {
						Node node = ring.get(i);
						Node nextNode = ring.get(i + 1);

						if (genHash(selection).compareTo(node.node_id) > 0 && genHash(selection).compareTo(nextNode.node_id) < 0) {

							String msg = "Query:" + selection;
						String str =	new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, nextNode.port).get();
							Log.d(TAG, "QueryMethod:" + "Blocking Array Waiting");
							/*
							String str = null;
							try {
								str = QueryKeyVal.take();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}*/

							//Here get all the three values compare their timestamp and then add it to cursor and return

							String crkv[] = str.split("\\|");
							System.out.println("TOTAL LEGTH OF CRKV RECEIVED::"+crkv.length);
							for(int z=0;z<crkv.length;z++){
								System.out.println(crkv[z]);
							}
							ArrayList<String> keys = new ArrayList<String>();
							ArrayList<String> values = new ArrayList<String>();
							ArrayList<Long> time = new ArrayList<Long>();
							for(int j=0; j<crkv.length;j++){
								if(!crkv[j].equals("null") && !crkv[j].equals("") && !crkv[j].equals(null)) {
									System.out.println("Inside:"+crkv[j]);
									keys.add(crkv[j].split(":")[0]);
									values.add(crkv[j].split(":")[1].split("-")[0]);
									time.add(Long.parseLong(crkv[j].split(":")[1].split("-")[1]));
								}
								}

							//Find maximum time value index
							int m = time.indexOf(Collections.max(time));

							Log.d(TAG, "QueryMethod:" + "Blocking Array Got Value");
							String k = keys.get(m);
							String v = values.get(m);
							mc.addRow(new String[]{k, v});

							//QueryKeyVal.clear();
							return mc;

						}
					}
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		return null;
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

	public String getMyPort() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		String myportid = myPort + ":" + portStr;
		return myportid;  //11108:5554
	}

	public Cursor queryKeys(Uri uri, String[] projection, String selection,
							String[] selectionArgs, String sortOrder){
		MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});
		Context con = getContext();
		String data;
		Log.d(TAG, "QueryMethod: Selection:" + selection);
		if (selection.contains("@")) {
			for (int i = 0; i < Files.size(); i++) {
				try {
					FileInputStream fis = con.openFileInput(Files.get(i));
					BufferedReader br = new BufferedReader(new InputStreamReader(fis));
					while ((data = br.readLine()) != null) {
						mc.addRow(new String[]{Files.get(i), data});

					}
					fis.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return mc;
		}
		return null;
	}

	class Node implements Comparable<Node> {
		String node_id;
		Node predecessor;
		Node successor;
		String port;   //5554
		String portno; //11108


		public String getNode_id() {
			return node_id;
		}

		public String getPort() {
			return port;
		}

		public Node getPredecessor() {
			return predecessor;
		}

		public void setPredecessor(Node predecessor) {
			this.predecessor = predecessor;
		}

		public Node getSuccessor() {
			return successor;
		}

		public void setSuccessor(Node successor) {
			this.successor = successor;
		}

		public String getPortno() {
			return portno;
		}

		public Node(String node_id, Node predecessor, Node successor, String port, String portno) {
			this.node_id = node_id;
			this.predecessor = predecessor;
			this.successor = successor;
			this.port = port;
			this.portno = portno;

		}


		public int compareTo(Node o) {
			return this.node_id.compareTo(o.node_id);
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {

			ServerSocket serverSocket = serverSockets[0];
			try {
				while (true) {
					Socket socket = serverSocket.accept();
					InputStreamReader isr = new InputStreamReader(socket.getInputStream());
					BufferedReader br = new BufferedReader(isr);
					String msg = br.readLine();
					Log.d(TAG, "Message at server:" + msg);

					if(msg.contains("Insert")){
						//synchronized (Lock) {
							Log.e("INSERT IN",""+myavdPort);
							String val[] = msg.split(":");
							ContentValues cv = new ContentValues();
							cv.put("key", val[1]);
							cv.put("value", val[2]);
							insert(mUri, cv);
						//}

					}

					else if(msg.contains("Query")){
						synchronized (Lock) {
							try{
								sleep(20);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}

							String k = msg.split(":")[1];
							Log.d(TAG, "Reveived Query Request at::" + myavdPort +":"+k);
							//String hk = genHash(k);
							//Check in Files to get key-value and send back
							String data;
							String found = "";
							Context con = getContext();
							for (int i = 0; i < Files.size(); i++) {
								if (k.equals(Files.get(i))) {

									try {
										FileInputStream fis = con.openFileInput(Files.get(i));
										BufferedReader br1 = new BufferedReader(new InputStreamReader(fis));

										data = br1.readLine();
										found += k;
										found += ":" + data;

										fis.close();
									} catch (FileNotFoundException e) {
										e.printStackTrace();
									} catch (IOException e) {
										e.printStackTrace();


									}
									break;
								}
							}
							Log.d(TAG, "Server side:Checking in my files done");

							Log.d(TAG, "Server side: Found Value for" + k);
							//When Key found the msg1 -> Found#Key:Value
							String msg1 = found;
							DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
							dos.writeBytes(msg1 + "\n");
							dos.flush();
							Log.d(TAG, "Server side:  Found Value: Sent");


						}
					}
					else if(msg.contains("ALLKVPAIRS")){
						String kv = "";
						Log.d(TAG, "Calling Cursor: Server Side");
						Cursor c = query(mUri, null, "@", null, null, null);
						try {
							while (c.moveToNext()) {
								Log.d(TAG, "Iterating over Cursor: Server Side");
								String k = c.getString(c.getColumnIndex("key"));
								String v = c.getString(c.getColumnIndex("value"));
								kv += k;
								kv += ":" + v;
								kv += "|";
							}

						} finally {
							c.close();
						}

						Log.d(TAG, "Sending my Content Provider Data from Server Side" + kv);
						DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
						dos.writeBytes(kv +"\n");
						dos.flush();
						Log.d(TAG,"My key-value pairs sent"+myavdPort);

					}
					else if(msg.contains("Delete")){
						Log.e("SERVER","DELETE");
						String key = msg.split(":")[1];
						String []r = {"rep"};
						delete(mUri,key,r);
						Log.e("DELETED::",""+key);
					}else if(msg.contains("Recovery")){

						//Cursor c = query(mUri,null,"fail",null,null,null);
						String kv = "";
						Log.d(TAG, "GOT RECOVERY REQUEST");

						Log.d(TAG, "Calling Cursor: Server Side");
						Cursor c = queryKeys(mUri, null, "@", null, null);
						try {
							while (c.moveToNext()) {
								Log.d(TAG, "Iterating over Cursor: Server Side");
								String k = c.getString(c.getColumnIndex("key"));
								String v = c.getString(c.getColumnIndex("value"));
								kv += k;
								kv += ":" + v;
								kv += "|";
							}

						} finally {
							c.close();
						}

						Log.d(TAG, "Sending my Content Provider Data from Server Side" + kv);
						DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
						dos.writeBytes(kv + "\n");
						dos.flush();
						Log.d(TAG, "My key-value pairs sent" + myavdPort);

					}

				}
			}catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}
	private class ClientTask extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... msgs) {
			Log.d(TAG, "Welcome to Client Task");
			String msg = msgs[0];
			Log.d("MESSAGE For SERVER:", "MSG:"+msg);
			String port = msgs[1];
			Log.d(TAG, "Possible port to send: "+port);

			if(msg.contains("Insert")){

				Socket socket = null;
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port)*2);
					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					dos.writeBytes(msg + "\n");
					dos.flush();
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				ArrayList<Node> replica = getReplicaNodes(port);

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(replica.get(0).portno));
					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					dos.writeBytes(msg + "\n");
					dos.flush();
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(replica.get(1).portno));
					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					dos.writeBytes(msg + "\n");
					dos.flush();
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}


			}
			else if(msg.contains("Query")){
				Log.e("GETTING QUERY RESULT:",""+msg);
				String result="";
				Socket socket = null;
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port)*2);
					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					dos.writeBytes(msg + "\n");
					dos.flush();

					InputStreamReader isr = new InputStreamReader(socket.getInputStream());
					BufferedReader br = new BufferedReader(isr);
					String msg1 = br.readLine();
					result += msg1;
					result +="|";
					System.out.println("GOT at 1"+result);

					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				ArrayList<Node> replica = getReplicaNodes(port);
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(replica.get(0).portno));
					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					dos.writeBytes(msg + "\n");
					dos.flush();

					InputStreamReader isr = new InputStreamReader(socket.getInputStream());
					BufferedReader br = new BufferedReader(isr);
					String msg1 = br.readLine();
					result += msg1;
					result +="|";
					System.out.println("GOT at 2"+result);

					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(replica.get(1).portno));
					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					dos.writeBytes(msg + "\n");
					dos.flush();

					InputStreamReader isr = new InputStreamReader(socket.getInputStream());
					BufferedReader br = new BufferedReader(isr);
					String msg1 = br.readLine();
					result += msg1;
					result +="|";
					System.out.println("GOT at 3"+result);

					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				/*
				try {
					QueryKeyVal.put(result);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}*/
				System.out.println("RETURNING:::::::::"+result);
				return result;
			}
			else if(msg.contains("ALLKVPAIRS")) {
				String result = "";
				Socket socket = null;
				String portno = null;
				for (Node n : ring) {
					if (n.port.equals(myavdPort)) {
						portno = n.portno;
						break;
					}
				}

				for (int i = 0; i < remotePort.size(); i++) {
					try {
						if (!remotePort.get(i).equals(portno)) {
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(remotePort.get(i)));
							socket.setSoTimeout(1500);
							DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
							dos.writeBytes(msg + "\n");
							dos.flush();

							InputStreamReader isr = new InputStreamReader(socket.getInputStream());
							BufferedReader br = new BufferedReader(isr);
							String msg1;
							if ((msg1 = br.readLine()) != null) {

								result += msg1;
							}
							socket.close();


						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				try{
					AllKeyVal.put(result);

			} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else if(msg.contains("Delete")){
				ArrayList<Node> replica = getReplicaNodes(port);
				Socket socket=null,socket1=null;
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(replica.get(0).portno));
					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					dos.writeBytes(msg + "\n");
					dos.flush();
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				try {
					socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(replica.get(1).portno));
					DataOutputStream dos = new DataOutputStream(socket1.getOutputStream());
					dos.writeBytes(msg + "\n");
					dos.flush();
					socket1.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}else if(msg.contains("Recovery")) {
				//Get the node from the ring corresponding to this AVD

				Log.e("STARTING RECOVERY :", "" + myavdPort);
				Log.e("MSG to Send", msg);
				String result = "";
				boolean pred1 = false;
				boolean succ1 = false;
				Node thisNode = null;
				for (int i = 0; i < ring.size(); i++) {
					if (myavdPort.equals(ring.get(i).port)) {
						thisNode = ring.get(i);
						break;
					}
				}
				//Now as we have Node for this avd we can access its predecessors and successors.
				for (int i = 0; i < remotePort.size(); i++) {
					try {
						if (!thisNode.portno.equals(remotePort.get(i))) {
							Log.e("Connecting to:", ring.get(i).portno);
							Socket socketnew = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(remotePort.get(i)));
							//socketnew.setSoTimeout(2000);
							DataOutputStream dout = new DataOutputStream(socketnew.getOutputStream());
							dout.writeBytes(msg + "\n");
							dout.flush();
							Log.e("Recovery", "Sent Message for recovery from client");
							//Get Key-Value Pair From the Successor Cursor in String form with some delimiter as well as its successor and update the send port with successor.

							InputStreamReader isr = new InputStreamReader(socketnew.getInputStream());
							BufferedReader br = new BufferedReader(isr);
							String msg1 = br.readLine();
							Log.e("Here Yaar", msg1);

							result += msg1;


						}
					} catch (SocketException e) {
						e.printStackTrace();
					} catch (IOException e) {

						e.printStackTrace();
					} catch (NullPointerException e) {
						e.printStackTrace();
					}


				}

				Node p1 = thisNode.getPredecessor();
				Node p2 = thisNode.getPredecessor().getPredecessor();
				Node s1 = thisNode.getSuccessor();
				Node s2 = thisNode.getSuccessor().getSuccessor();
				Log.e("Details::",""+thisNode.port+":"+p1.port+":"+p2.port);

				//Now Got the key-value pairs in result for recovery  //Now Write the recovery code at server
				HashMap<String, String> kvmap = new HashMap<String, String>();
				Log.e("MAP", "" + kvmap.size());
				Log.e("RESULT:::",""+result +"@@END");
				String kvp[] = result.split("\\|");
				String k;
				String v;
				Log.e("KVPLENGTH", "" + kvp.length);
				String test = "";
				for (int i = 0; i < kvp.length; i++) {
					System.out.println(kvp[i]);
					test += kvp[i];
				}

				Log.e("KVP:::", "hello" + test);
				if (kvp.length > 1) {
					for (int i = 0; i < kvp.length; i++) {
						Log.e("KVP", kvp[i]);
						k = kvp[i].split(":")[0];
						v = kvp[i].split(":")[1];
						Log.e("1097:K", k);
						Log.e("1098:V", v);
						Node cnode = getCoordinatorNode(k);
						if (thisNode.port.equals(cnode.port) || p1.port.equals(cnode.port) || p2.port.equals(cnode.port)) {
							if (!kvmap.containsKey(k)) {
								kvmap.put(k, v);
							} else {
								String val = kvmap.get(k);
								if (v.split("-")[1].compareTo(val.split("-")[1]) > 0) {
									kvmap.put(k, v);
								}
								else{
									kvmap.put(k,val);
								}
							}
						}
					}
				}
				Log.e("PREPARING MAP::",""+kvmap.size());
				synchronized (Lock) {
					for (String key : kvmap.keySet()) {
						ContentValues values = new ContentValues();
						values.put("key", key);
						values.put("value", kvmap.get(key));
						Log.e("INSERTING INTO MAP:",key+":"+kvmap.get(key).split("-")[0]);
						insert(mUri, values);
					}
					Log.e("MAP SIZE::", "" + kvmap.size());
				}
				recoveryCompleted = true;
				//return "Completed";

			}

			return null;
		}
	}

	public ArrayList<Node> getReplicaNodes(String port){
		ArrayList<Node> replica = new ArrayList<Node>();
		for(int i =0; i<ring.size();i++){
			if(ring.get(i).port.equals(port)){
				if(i<=2){
					replica.add(ring.get(i+1));
					replica.add(ring.get(i+2));
					return replica;
				}
				else if(i==3){
					replica.add(ring.get(i+1));
					replica.add(ring.get(0));
					return replica;
				}
				else if(i==4){
					replica.add(ring.get(0));
					replica.add(ring.get(1));
					return replica;
				}
			}
		}
		return replica;
	}

	public Node getCoordinatorNode(String key){

		Node first = ring.get(0);
		Node last = ring.get(ring.size()-1);
		try {
			if(genHash(key).compareTo(first.node_id)<0 || genHash(key).compareTo(last.node_id)>0){
				return first;
			}
			for(int i=0;i<ring.size()-1;i++){
				Node node = ring.get(i);
				Node nextNode = ring.get(i+1);
				if(genHash(key).compareTo(node.node_id)>0 && genHash(key).compareTo(nextNode.node_id)<0){
					return nextNode;
				}
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	}


