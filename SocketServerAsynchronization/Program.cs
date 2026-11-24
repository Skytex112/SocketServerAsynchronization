
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace SocketServerAsynchronization
{
    internal class Program
    {
        static List<Socket> clients = new List<Socket>();              // все клиенты
        static List<string> messages = new List<string>();             // общие сообщения
        static object locker = new object();
        static void Main(string[] args)
        {
            using Socket serverSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            //using Socket serverSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            //int counter = 0;

            serverSocket.Bind(new IPEndPoint(IPAddress.Any, 5000));
            serverSocket.Listen(10);

            byte[] buffer = new byte[1024];

            //EndPoint clientEP = new IPEndPoint(IPAddress.Any, 0);

            while (true)
            {
                Socket clientSocket = serverSocket.Accept();

                Task.Run(() => HandleUser(clientSocket));

                /*
                serverSocket.Bind(new IPEndPoint(IPAddress.Loopback, 5000));
                serverSocket.Listen(20);
                Console.WriteLine("Server started , waiting for connecting.");

                while (true)
                {
                    Socket clientSocket = serverSocket.Accept();

                    Console.WriteLine("Client connected!");
                    Task.Run(() => HandleClient(clientSocket));


                }
                serverSocket.Close();*/
            }
        }

        private static async Task HandleUser(Socket client)
        {
            // читаем юзернейм
            var buffer = new byte[1024];
            int received = client.Receive(buffer);
            string username = Encoding.UTF8.GetString(buffer, 0, received);

            Console.WriteLine($"Client: {username} connected");

            // добавляем клиента в список
            lock (locker)
            {
                clients.Add(client);
            }

            try
            {
                while (true)
                {
                    buffer = new byte[1024];
                    int bytes = await client.ReceiveAsync(buffer, SocketFlags.None);
                    if (bytes == 0) break; 

                    string message = Encoding.UTF8.GetString(buffer, 0, bytes);

                    if (message == "UPD")
                    {
                        
                        string allMessages;
                        lock (locker)
                        {
                            allMessages = string.Join(Environment.NewLine, messages);
                        }

                        byte[] sendBuffer = Encoding.UTF8.GetBytes(allMessages);
                        await client.SendAsync(sendBuffer, SocketFlags.None);
                    }
                    else
                    {
                        string fullMessage = $"[{username}]: {message}";
                        Console.WriteLine(fullMessage);

                        
                        lock (locker)
                        {
                            messages.Add(fullMessage);
                        }

                    }
                }
            }
            finally
            {
                lock (locker)
                {
                    clients.Remove(client);
                }
                client.Close();
                Console.WriteLine($"Client {username} disconnected");
            }
        }
        private static void HandleClient(Socket client)
        {
            try
            {
                var buffer = new byte[1024];
                while (true)
                {
                    int bytes = client.Receive(buffer);
                    if (bytes == 0) break;
                    string msg = Encoding.UTF8.GetString(buffer, 0, bytes);
                    Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}] {msg}");
                    client.Send(Encoding.UTF8.GetBytes("Echo: " + msg));
                }

            }
            catch (SocketException ex) { Console.WriteLine($"Error Socket: {ex.Message}"); }
            finally { client.Close(); Console.WriteLine("Client disconnected"); }
        }

        private static async Task HandleClientAsync(Socket client)
        {
            byte[] buffer = new byte[1024];
            try
            {
                while (true)
                {
                    int bytes = await client.ReceiveAsync(buffer, SocketFlags.None);
                    if (bytes == 0) break;

                    string msg = Encoding.UTF8.GetString(buffer, 0, bytes);
                    Console.WriteLine($"[{Environment.CurrentManagedThreadId}] {msg}");

                    byte[] echo = Encoding.UTF8.GetBytes("Echo: " + msg);
                    await client.SendAsync(echo, SocketFlags.None);
                }
            }
            catch ( SocketException ex) {
                Console.WriteLine($"Error Socket: {ex.Message}"); }
            finally { client.Close(); Console.WriteLine("Client disconnected"); }
        }
    }
}
