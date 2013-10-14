%% Copyright
-module(client).
-author("Matthias Stellmann").

%% API
-export([startClients/1]).

%% Start all Clients
startClients(ServerNode) ->
% Read the cfg-File
  {ok, ConfigList} = file:consult("client.cfg"),
  {ok, NumberOfClients} = werkzeug:get_config_value(clients, ConfigList),
  {ok, LifetimeInSeconds} = werkzeug:get_config_value(lifetime, ConfigList),
  {ok, SendintervalInSeconds} = werkzeug:get_config_value(sendeintervall, ConfigList),
  {ok, Servername} = werkzeug:get_config_value(servername, ConfigList),

  ServerPID = {Servername, ServerNode},
  io:format("Serverinfo: Server at ~p with PID ~p~n", [ServerNode, ServerPID]),

  Sendinterval = timer:seconds(SendintervalInSeconds),
  Lifetime = timer:seconds(LifetimeInSeconds),
  createClients(lists:seq(1, NumberOfClients), ServerPID, Sendinterval, Lifetime).

%% This Function spawnes the clients
%% returns a List of the spawend PID for each Client
createClients(ClientIDList, ServerPID, Sendinterval, Lifetime) ->
  lists:map(fun(ClientID) ->
    ClientPID = spawn(fun() -> editor(ClientID, ServerPID, Sendinterval, 0, []) end),
    io:format("INFO: Client with NR: ~p PID: ~p startet at ~p~n", [ClientID, ClientPID, werkzeug:timeMilliSecond()]),
    timer:send_after(Lifetime, ClientPID, endOfLifetime),
    ClientPID end,
%% 2. Parameter of the lists:map/2 Function
    ClientIDList)
.

%% editorMode - asking the Server for new MessageNumbers and sending Messages to the Server
editor(ClientID, ServerPID, Sendinterval, NumberOfSendMsg, OwnMessageNumbers) ->
  ServerPID ! {getmsgid, self()},
  receive
    endOfLifetime -> exitClient(ClientID);

    {nnr, Number} ->
%% After sending 5 Messages, forget to send a Message and switch to Readermode
      case NumberOfSendMsg of
        5 ->
          logToFile(ClientID, lists:concat(["forgot to send MessageNr: ",Number," at",werkzeug:timeMilliSecond() ," ~n"])),
          reader(ClientID, ServerPID, OwnMessageNumbers, Sendinterval);
        _ ->
%% lists:concat([Number, ".Message: ", getHost()," ", ClientNr," G2/T13", " Send: ", werkzeug:timeMilliSecond()])
          Message = lists:concat([Number, ".-Message: C out - ", werkzeug:timeMilliSecond(), "(", getHostname(), ") ;"]),
          ServerPID ! {dropmessage, {Message, Number}},
          logToFile(ClientID, lists:concat(["send Message: ", Message,"~n"])),
          timer:sleep(Sendinterval),
          editor(ClientID, ServerPID, Sendinterval, NumberOfSendMsg + 1, lists:append(OwnMessageNumbers, [Number]))
      end;

    Any ->
      logToFile(ClientID, lists:concat(["received anything not understandable: ", Any, "~n"])),
      editor(ClientID, ServerPID, Sendinterval, NumberOfSendMsg, OwnMessageNumbers)
  end
.

%% readerMode - receiving Messages from Server and saving them into the .log-File
reader(ClientID, ServerPID, OwnMessageNumbers, SendInterval) ->
  ServerPID ! {getmessages, self()},
  receive
    endOfLifetime -> exitClient(ClientID);

    {reply, NNr, Message, Termi} ->
%% checking if received Message is from our own editor
      case lists:any(fun(Elem) -> Elem == NNr end, OwnMessageNumbers) of
        true -> logToFile(ClientID, lists:concat(["Received a Message of my own: ",Message , "at ",werkzeug:timeMilliSecond(),"~n"]));
        false -> logToFile(ClientID, lists:concat(["Received a Message: ",Message , "at ",werkzeug:timeMilliSecond(),"~n"]))
      end,
      case Termi of
%% switching back to editormode if Termi is true with a new calculated SendInterval
        true -> editor(ClientID, ServerPID, calculateNewTimeInterval(SendInterval), 0, OwnMessageNumbers);
%% more messages to receive
        false -> reader(ClientID, ServerPID, OwnMessageNumbers, SendInterval)
      end;

    Any ->
      logToFile(ClientID, lists:concat(["received anything not understandable: ", Any, "~n"])),
      reader(ClientID, ServerPID, OwnMessageNumbers, SendInterval)
  end
.

%% Exits the given Client and logs it
exitClient(ClientID) ->
  io:format("INFO: Client with NR: ~p PID: ~p ended at ~p~n", [ClientID, self(), werkzeug:timeMilliSecond()]),
  erlang:exit("EndOfLife")
.

%% Generates a new Timeinterval
%% generated interval is always 50 percent '<' or '>' as the 'CurrentInterval' with a minimum of +-1
calculateNewTimeInterval(CurrentInterval) ->
  UpOrDown = random:uniform(2),
  if CurrentInterval >= 2 ->
    if UpOrDown == 1 -> CurrentInterval * 0.5;
      UpOrDown == 2 -> CurrentInterval * 1.5
    end;
    CurrentInterval < 2 ->
      if CurrentInterval == 1 -> 1 + random:uniform(7);
        UpOrDown == 1 -> 1;
        UpOrDown == 2 -> checkNewTimeInterval(CurrentInterval, CurrentInterval * 1.5)
      end
  end
.

%% subFunction for calculateNewTimeInterval/1
%% provides the +-1 - Rule
checkNewTimeInterval(CurrentInterval, NewInterval) ->
  if abs(CurrentInterval - NewInterval) >= 1 -> NewInterval;
    abs(CurrentInterval - NewInterval) < 1 ->
      if NewInterval > CurrentInterval -> NewInterval + (1 - abs(CurrentInterval - NewInterval));
        NewInterval < CurrentInterval -> NewInterval - (1 - abs(CurrentInterval - NewInterval))
      end
  end
.

getHostname() ->
  {ok, Hostname} = inet:gethostname(),
  Hostname
.

%% simplifies the Logging
logToFile(ClientNr, Message) ->
  Filename = lists:concat(["client_", ClientNr, "@", getHostname(), ".log"]),
  werkzeug:logging(Filename, lists:concat(["[", werkzeug:timeMilliSecond(), "] ", ClientNr, ": ", Message]))
.