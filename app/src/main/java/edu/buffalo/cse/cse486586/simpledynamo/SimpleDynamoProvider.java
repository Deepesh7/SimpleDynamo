package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;

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
	public HashMap<String, String[]> partitions = new HashMap<String, String[]>();
	public HashMap<String, String[]> predecessor = new HashMap<String, String[]>();
	static final int SERVER_PORT = 10000;
	String myPort = "";
	String node_id = "";
	String recieved_val = "-1";
	String star_text = "";
	boolean star_res = false;
	String[] Membership_ports = {"5562", "5556", "5554", "5558", "5560"};
	ArrayList<String> Membership_node_ids = new ArrayList<String>();
	public HashMap<String, String> data = new HashMap<String, String>();
	public HashMap<String, String> first_replica = new HashMap<String, String>();
	public HashMap<String, String> second_replica = new HashMap<String, String>();
	ArrayList<HashMap> all_data = new ArrayList<HashMap>();
	public static final String TAG = "IS_FAILED";
	volatile boolean data_check = false;
	String recieved_data = "";


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		try {
			for (int i = 0; i < all_data.size(); i++) {
				HashMap<String, String> d = all_data.get(i);
				d.remove(selection);
			}
			String hash_key = genHash(selection);
			String coordinator = calcCoordinator(hash_key);
			String msgToSend = "DELETE__" + coordinator + "__" + selection;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String key = (String) values.get("key");
		try {
			String hash_key = genHash(key);
			String coordinator = calcCoordinator(hash_key); //Calculate co-ordinator
			Log.i("Hello insert value", String.valueOf(values));
			Log.i("Hello coordinator", String.valueOf(coordinator));
			String msgToSend = "";
			msgToSend = "INSERT__" + coordinator + "__" + values.get("key") + "--" + values.get("value");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


		return null;
	}

	private String calcCoordinator(String key) {
		int i = 0;
		for (i = 0; i < Membership_node_ids.size(); i++) {
			if (Membership_node_ids.get(i).compareTo(key) > 0) {
				break;
			}
		}
		String coordinator = String.valueOf(Integer.parseInt(Membership_ports[i % 5]) * 2);

		return coordinator;
//		String[] prefList = {String.valueOf(Integer.parseInt(Membership_ports[i%5]) * 2),
//							 String.valueOf(Integer.parseInt(Membership_ports[(i+1)%5]) * 2),
//				   			 String.valueOf(Integer.parseInt(Membership_ports[(i+2)%5]) * 2)};

//		return prefList;

	}


	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e("Hello", "Can't create a ServerSocket");
		}
		all_data.add(data);
		all_data.add(first_replica);
		all_data.add(second_replica);
		partitions.put("11108", new String[]{"11116", "11120"});
		partitions.put("11112", new String[]{"11108", "11116"});
		partitions.put("11116", new String[]{"11120", "11124"});
		partitions.put("11120", new String[]{"11124", "11112"});
		partitions.put("11124", new String[]{"11112", "11108"});

		predecessor.put("11108", new String[]{"11112", "11124"});
		predecessor.put("11112", new String[]{"11124", "11120"});
		predecessor.put("11116", new String[]{"11108", "11112"});
		predecessor.put("11120", new String[]{"11116", "11108"});
		predecessor.put("11124", new String[]{"11120", "11116"});

		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		Log.i("Hello", portStr);

		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		Log.i("Hello", myPort);

		SharedPreferences sharedPreferences = getContext().getSharedPreferences("Failure_state", Context.MODE_PRIVATE);
		boolean has_failed = sharedPreferences.getBoolean(TAG, false);
		SharedPreferences.Editor editor = sharedPreferences.edit();
		editor.putBoolean(TAG, true);
		editor.apply();
		Log.i("Hello", "has failed: " + has_failed);
		if (has_failed) {
			Log.i("Hello", "Recovery mode");
			getData();
			Log.i("Hello", "Recovery task over");
		}
		Log.i("Create", "Launching Content Provider");
		Log.i("Create data", String.valueOf(data));
		Log.i("Create first_replica", String.valueOf(first_replica));
		Log.i("Create second replica", String.valueOf(second_replica));

		try {
			for (int i = 0; i < Membership_ports.length; i++) {
				Membership_node_ids.add(genHash(Membership_ports[i]));
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		Log.i("Hello port String", portStr);
		Log.i("Hello node id", node_id);
		Log.i("Hello Mem node ids", String.valueOf(Membership_node_ids));

		return false;
	}

	public void getData() {
		String msgToSend = "SEND__REPLICA1__" + myPort;
		Log.i("Hello recovery msg", msgToSend);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
		while (!data_check) {
			//Log.i("Recovery","waiting replica1");
		}
		Log.i("Reocvery", "loop over after receiving replica1.");
		Log.i("Recovery rcvd replica1", recieved_data);
		if (!recieved_data.equals("")) {
			try {
				String arr[] = recieved_data.split("--");
				for (int i = 0; i < arr.length; i++) {
					String[] key_val = arr[i].split("@");
					if (data.containsKey(key_val[0])) {
						continue;
					} else {
						data.put(key_val[0], key_val[1]);
					}

				}
			} catch (Exception e) {
				Log.i("query", "Error occured from the star_text val");
			}
		}
		data_check = false;
		recieved_data = "";

		msgToSend = "SEND__REPLICA2__" + myPort;
		Log.i("Hello recovery msg", msgToSend);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
		while (!data_check) {
			//Log.i("Recovery","waiting replica2");
		}
		Log.i("Reocvery", "loop over after receiving replica2.");
		Log.i("Recovery rcvd replica2", recieved_data);

		if (!recieved_data.equals("")) {
			try {
				String arr[] = recieved_data.split("--");
				for (int i = 0; i < arr.length; i++) {
					String[] key_val = arr[i].split("@");
					if (data.containsKey(key_val[0])) {
						continue;
					} else {
						data.put(key_val[0], key_val[1]);
					}

				}
			} catch (Exception e) {
				Log.i("query", "Error occured from the star_text val");
			}
		}
		data_check = false;
		recieved_data = "";

		msgToSend = "SEND__DATA1__" + myPort;
		Log.i("Hello recovery msg", msgToSend);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
		while (!data_check) {
			//Log.i("Recovery","waiting data1");
		}
		Log.i("Reocvery", "loop over after receiving data1.");
		Log.i("Recovery rcvd replica2", recieved_data);
		if (!recieved_data.equals("")) {
			try {
				String arr[] = recieved_data.split("--");
				for (int i = 0; i < arr.length; i++) {
					String[] key_val = arr[i].split("@");
					if (first_replica.containsKey(key_val[0])) {
						continue;
					} else {
						first_replica.put(key_val[0], key_val[1]);
					}

				}
			} catch (Exception e) {
				Log.i("query", "Error occured from the star_text val");
			}
		}
		data_check = false;
		recieved_data = "";

		msgToSend = "SEND__DATA2__" + myPort;
		Log.i("Hello recovery msg", msgToSend);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
		while (!data_check) {
			//Log.i("Recovery","waiting data2");
		}
		Log.i("Reocvery", "loop over after receiving data2.");
		Log.i("Recovery rcvd data2", recieved_data);
		if (!recieved_data.equals("")) {
			try {
				String arr[] = recieved_data.split("--");
				for (int i = 0; i < arr.length; i++) {
					String[] key_val = arr[i].split("@");
					if (second_replica.containsKey(key_val[0])) {
						continue;
					} else {
						second_replica.put(key_val[0], key_val[1]);
					}

				}
			} catch (Exception e) {
				Log.i("query", "Error occured from the star_text val");
			}
		}
		data_check = false;
		recieved_data = "";

		Log.i("Hello recover", "data recieved");
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
									 String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		String colnames[] = new String[]{"key", "value"};
		MatrixCursor matrixCursor = new MatrixCursor(colnames);

		if (selection.equals("@")) {
			for (int i = 0; i < all_data.size(); i++) {
				HashMap<String, String> d = all_data.get(i);
				for (String key : d.keySet()) {
					Log.i("hello query @", key);
					String val = d.get(key);
					matrixCursor.addRow(new Object[]{key, val});
				}
			}
			return matrixCursor;
		}
		if (selection.equals("*")) {
			HashMap<String, String> matrixCursorMap = new HashMap<String, String>();
			for (int i = 0; i < all_data.size(); i++) {
				HashMap<String, String> d = all_data.get(i);
				for (String key : d.keySet()) {
					Log.i("hello query *", key);
					String val = d.get(key);
					matrixCursorMap.put(key, val);
				}
			}

			//Send to all other clients and recieve data.

			for (String port : partitions.keySet()) {
				String msgToSend = "QUERY*__" + myPort;
				if (port.equals(myPort)) {
					continue;
				} else {
					msgToSend += "__" + port;
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
					long st = System.currentTimeMillis();
					long et = st + 1000;
					while (!star_res) {
						if (System.currentTimeMillis() > et) {
							break;
						}
						//Log.i("Query","waiting System time: "+System.currentTimeMillis()+" End time:"+et);
					}
					if (!star_text.equals("")) {
						try {
							String arr[] = star_text.split("--");
							for (int i = 0; i < arr.length; i++) {
								String[] key_val = arr[i].split("@");
								if (matrixCursorMap.containsKey(key_val[0])) {
									continue;
								} else {
									matrixCursorMap.put(key_val[0], key_val[1]);
									//matrixCursor.addRow(new Object[]{key_val[0], key_val[1]});
								}

							}
						} catch (Exception e) {
							Log.i("query", "Error occured from the star_text val");
						}
					}
				}
				star_res = false;
				star_text = "";
			}
			for (String key : matrixCursorMap.keySet()) {
				String val = matrixCursorMap.get(key);
				matrixCursor.addRow(new Object[]{key, val});
			}

			return matrixCursor;
		} else {
			if (data.containsKey(selection)) {
				recieved_val = data.get(selection);
			} else if (first_replica.containsKey(selection)) {
				recieved_val = first_replica.get(selection);
			} else {
				recieved_val = second_replica.get(selection);
			}


			if (recieved_val == null) {
				Log.i("query", "not in my node");
				String hash_key = null;
				try {
					hash_key = genHash(selection);
					String coordinator = calcCoordinator(hash_key); //Calculate coordinatoqqr
					Log.i("Hello query key", selection);
					Log.i("Hello query coordinator", coordinator);

					String msgToSend = "QUERY__" + coordinator + "__" + selection; //Sending query to coordinator.
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);

					recieved_val = "-1";
					long st = System.currentTimeMillis();
					long et = st + 1000;
					while (recieved_val.equals("-1")) {
						if (System.currentTimeMillis() > et) {
							break;
						}
					}
					Log.i("Query", "Recieved val: " + recieved_val);
					if (recieved_val.equals("-1")) {
						msgToSend = "QUERY__" + partitions.get(coordinator)[0] + "__" + selection;
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
					}
					st = System.currentTimeMillis();
					et = st + 1000;
					while (recieved_val.equals("-1")) {
						//Log.i("Query","waiting");
						if (System.currentTimeMillis() > et) {
							break;
						}
					}
					Log.i("Query", "Recieved val: " + recieved_val);
					if (recieved_val.equals("-1")) {
						msgToSend = "QUERY__" + partitions.get(coordinator)[1] + "__" + selection;
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend);
					}
					st = System.currentTimeMillis();
					et = st + 1000;
					while (recieved_val.equals("-1")) {
						//Log.i("Query","waiting");
						if (System.currentTimeMillis() > et) {
							break;
						}
					}
					Log.i("Query", "Recieved val: " + recieved_val);
					if (recieved_val.equals("-1")) {
						Log.i("ERROR", "Can't find the value bc");
					}
					Log.i("Hello loop over", recieved_val);
					matrixCursor.addRow(new Object[]{selection, recieved_val});

				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			} else {
				matrixCursor.addRow(new Object[]{selection, recieved_val});
			}
			return matrixCursor;
		}
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


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try {
				while (true) {
					Socket s = serverSocket.accept();
					ObjectInputStream in = new ObjectInputStream(s.getInputStream());
					String msg = String.valueOf(in.readObject());
					Log.i("Hello recieved message", msg);
					String flag_msg[] = msg.split("__");

					if (flag_msg[0].equals("DATA")) {
						Log.i("Hello", "Recieved data");
						String[] keyval = flag_msg[1].split("--");
						Log.i("Hello keyval", keyval[0] + " " + keyval[1]);
						data.put(keyval[0], keyval[1]);
					} else if (flag_msg[0].equals("REPLICA")) {
						if (flag_msg[1].equals("1")) {
							Log.i("Hello", "Recieved 1st Replica");
							String[] keyval = flag_msg[2].split("--");
							Log.i("Hello keyval", keyval[0] + " " + keyval[1]);
							first_replica.put(keyval[0], keyval[1]);
						} else if (flag_msg[1].equals("2")) {
							Log.i("Hello", "Recieved 2nd Replica");
							String[] keyval = flag_msg[2].split("--");
							Log.i("Hello keyval", keyval[0] + " " + keyval[1]);
							second_replica.put(keyval[0], keyval[1]);
						} else {
							Log.i("Hello", "ERROR UNREACHABLE CODE REACHED");
						}
					} else if (flag_msg[0].equals("DELETE")) {
						for (int i = 0; i < all_data.size(); i++) {
							HashMap<String, String> d = all_data.get(i);
							d.remove(flag_msg[1]);
						}
					} else if (flag_msg[0].equals("QUERY")) {
						String val = null;
						if (data.containsKey(flag_msg[1])) {
							val = data.get(flag_msg[1]);
						} else if (first_replica.containsKey(flag_msg[1])) {
							val = first_replica.get(flag_msg[1]);
						} else if (second_replica.containsKey(flag_msg[1])) {
							val = second_replica.get(flag_msg[1]);
						} else {
							Log.i("ERROR", "I don't have the value. Why am I being called?");
						}

						if (val != null) {
							String msgToSend = "QUERYREPLY__" + val;
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(flag_msg[2]));
							ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
							out.writeObject(msgToSend);
						}
					} else if (flag_msg[0].equals("QUERYREPLY")) {
						Log.i("Hreply", "value recieved: " + flag_msg[1]);
						recieved_val = flag_msg[1];
					} else if (flag_msg[0].equals("QUERY*")) {
						Log.i("Hello querry *", flag_msg[1] + "asked for query *");
						String msgToSend = "QUERYREPLY*__";
						for (int i = 0; i < all_data.size(); i++) {
							HashMap<String, String> d = all_data.get(i);
							for (String key : d.keySet()) {
								String val = d.get(key);
								msgToSend = msgToSend + key + "@" + val + "--";
							}
						}
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(flag_msg[1]));
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						out.writeObject(msgToSend);
					} else if (flag_msg[0].equals("QUERYREPLY*")) {
						Log.i("Hello Query* reply", "Reply recieved");
						star_text = flag_msg[1];
						star_res = true;
					} else if (flag_msg[0].equals("SEND")) {
						Log.i("Recovery", "Recovery request recieved");
						String msgToSend = "RECEIVE__";
						if (flag_msg[1].equals("REPLICA1")) {
							Log.i("Recovery", "Sending replica1");
							msgToSend += "REPLICA1__";
							for (String key : first_replica.keySet()) {
								String val = first_replica.get(key);
								msgToSend = msgToSend + key + "@" + val + "--";
							}
						} else if (flag_msg[1].equals("REPLICA2")) {
							Log.i("Recovery", "Sending replica2");
							msgToSend += "REPLICA2__";
							for (String key : second_replica.keySet()) {
								String val = second_replica.get(key);
								msgToSend = msgToSend + key + "@" + val + "--";
							}
						} else if (flag_msg[1].equals("DATA1") || flag_msg[1].equals("DATA2")) {
							Log.i("Recovery", "Sending data");
							msgToSend += "DATA__";
							for (String key : data.keySet()) {
								String val = data.get(key);
								msgToSend = msgToSend + key + "@" + val + "--";
							}
						}
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(flag_msg[2]));
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						out.writeObject(msgToSend);
						out.close();
						socket.close();
						Log.i("Recovery", "Data sent");
					} else if (flag_msg[0].equals("RECEIVE")) {
						Log.i("Recovery", "Recovery data recieved");
//						if(flag_msg[1].equals("REPLICA1") || flag_msg[1].equals("REPLICA2") || flag_msg[1].equals("DATA")){
						Log.i("Recovery", "DATA recieved");
						if (flag_msg.length == 3) {
							recieved_data = flag_msg[2];
						}
						data_check = true;
						Log.i("Recovery data", recieved_data);
						//}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			return null;
		}

		protected void onProgressUpdate(String... strings) {
			/*
			 * The following code displays what is received in doInBackground().
			 */
			return;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {

			String msgToSend = msgs[0];
			Log.i("Hello send message is: ", msgToSend);
			String[] flag_msg = msgToSend.split("__");
			Log.i("Hello flag is: ", flag_msg[0]);
			if (flag_msg[0].equals("INSERT")) {
				String coordinator = flag_msg[1];
				ArrayList<String> partition_ports = new ArrayList<String>();
				partition_ports.add(coordinator);
				partition_ports.add(partitions.get(coordinator)[0]);
				partition_ports.add(partitions.get(coordinator)[1]);
				Log.i("Hello partition", partition_ports.get(0) + " " + partition_ports.get(1) + " " + partition_ports.get(2));
				if (partition_ports.get(0).equals(myPort)) {
					Log.i("Hello", "I am the coordinator");
					String[] keyval = flag_msg[2].split("--");
					Log.i("Hello keyval", keyval[0] + " " + keyval[1]);
					data.put(keyval[0], keyval[1]);
				} else {
					try {
						Log.i("Insert", "Data sending to: " + partition_ports.get(0));
						Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(partition_ports.get(0)));
						ObjectOutputStream out1 = new ObjectOutputStream(socket1.getOutputStream());
						msgToSend = "DATA" + "__" + flag_msg[2];
						out1.writeObject(msgToSend);
						out1.close();
						socket1.close();
						Log.i("Insert", "Data sent to: " + partition_ports.get(0));
					} catch (Exception e) {
						Log.i("Insert", "Data sending failed for: " + partition_ports.get(0));
					}
				}
				if (partition_ports.get(1).equals(myPort)) {
					Log.i("Hello", "I am the 1st replica");
					String[] keyval = flag_msg[2].split("--");
					Log.i("Hello keyval", keyval[0] + " " + keyval[1]);
					first_replica.put(keyval[0], keyval[1]);
				} else {
					try {
						Log.i("Insert", "1st replica sending to: " + partition_ports.get(1));
						Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(partition_ports.get(1)));
						ObjectOutputStream out2 = new ObjectOutputStream(socket2.getOutputStream());
						msgToSend = "REPLICA__1" + "__" + flag_msg[2];
						out2.writeObject(msgToSend);
						out2.close();
						socket2.close();
						Log.i("Insert", "2nd replica sent to: " + partition_ports.get(1));
					} catch (Exception e) {
						Log.i("Insert", "1st replica sending failed for: " + partition_ports.get(1));
					}
				}
				if (partition_ports.get(2).equals(myPort)) {
					Log.i("Hello", "I am the 2nd replica");
					String[] keyval = flag_msg[2].split("--");
					Log.i("Hello keyval", keyval[0] + " " + keyval[1]);
					second_replica.put(keyval[0], keyval[1]);
				} else {
					try {
						Log.i("Insert", "2nd replica sending to: " + partition_ports.get(2));
						Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(partition_ports.get(2)));
						ObjectOutputStream out3 = new ObjectOutputStream(socket3.getOutputStream());
						msgToSend = "REPLICA__2" + "__" + flag_msg[2];
						out3.writeObject(msgToSend);
						out3.close();
						socket3.close();
						Log.i("Insert", "2nd replica sent to: " + partition_ports.get(2));
					} catch (Exception e) {
						Log.i("Insert", "2nd replica sending failed for: " + partition_ports.get(2));
					}
				}
			} else if (flag_msg[0].equals("DELETE")) {
				String coordinator = flag_msg[1];
				ArrayList<String> partition_ports = new ArrayList<String>();
				partition_ports.add(coordinator);
				partition_ports.add(partitions.get(coordinator)[0]);
				partition_ports.add(partitions.get(coordinator)[1]);
				for (int i = 0; i < partition_ports.size(); i++) {
					try {
						if (partition_ports.get(i).equals(myPort)) {
							continue;
						} else {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(partition_ports.get(i)));
							ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
							msgToSend = flag_msg[0] + "__" + flag_msg[2];
							out.writeObject(msgToSend);
							out.close();
							socket.close();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			} else if (flag_msg[0].equals("QUERY")) {
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(flag_msg[1]));
					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					msgToSend = flag_msg[0] + "__" + flag_msg[2] + "__" + myPort;
					Log.i("Query message", msgToSend);
					out.writeObject(msgToSend);
					Log.i("Query ", "Sent to port: " + flag_msg[1]);
					out.close();
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else if (flag_msg[0].equals("QUERY*")) {
				try {
					msgToSend = flag_msg[0] + "__" + flag_msg[1];
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(flag_msg[2]));
					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(msgToSend);
					out.close();
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else if (flag_msg[0].equals("SEND")) {
				String port = "";
				if (flag_msg[1].equals("REPLICA1")) {
					port = partitions.get(myPort)[0];
					Log.i("Recovery", "Asking for Replica1 from port: " + port);
				} else if (flag_msg[1].equals("REPLICA2")) {
					port = partitions.get(myPort)[1];
					Log.i("Recovery", "Asking for Replica2 from port: " + port);
				} else if (flag_msg[1].equals("DATA1")) {
					port = predecessor.get(myPort)[0];
					Log.i("Recovery", "Asking for Data1 from port: " + port);
				} else if (flag_msg[1].equals("DATA2")) {
					port = predecessor.get(myPort)[1];
					Log.i("Recovery", "Asking for Data2 from port: " + port);
				}
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(msgToSend);
					out.close();
					socket.close();
					Log.i("Recovery", "Recovery message sent");
					//data_check = true;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return null;
		}
	}
}
