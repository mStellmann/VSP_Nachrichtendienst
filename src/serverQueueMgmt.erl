%% Copyright
-module(serverQueueMgmt).
-author("Matthias Stellmann and Grzegorz Markiewicz").

%% API
-export([start/2]).

%% starting the QueueMgmt and initializing the queues
start(DLQLimit, ClientLifetime) ->
  erlang:spawn(fun() -> queueMgmt(DLQLimit, [], [], [], ClientLifetime) end)
.

%% receiving loop for the QueueMgmt
queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime) ->
  receive
    endOfLife -> exitQueueMgmt();

    {endOfClientLifeTime, ClientPID} ->
      logToFile(werkzeug:list2String(["endOfClientLifeTime with CliendPID: ", ClientPID])),
      clientMgmt_deleteClient(ClientPID, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit);

    {dropmessage, {Message, Number}} -> dropMessageInHBQ({Number, Message}, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit);

    {getmessages, ClientPID} ->
      LastSendMessagenumber = clientMgmt_lastMessageNumberSend(ClientPID, ClientList),
      NewSendMessagenumber = sendMessageToClient(ClientPID, LastSendMessagenumber, DLQ),
      logToFile(werkzeug:list2String(["getmessages with CliendPID: ", ClientPID, "LastMessageNumber: ", LastSendMessagenumber, "NewMessageNumber: ", NewSendMessagenumber])),
      clientMgmt_newMessageNumberOfClient(ClientPID, NewSendMessagenumber, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit);

    Any ->
      logToFile(lists:concat(["received anything: ", Any, "~n"])),
      queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime)
  end
.

%% function to drop the messages into the HBQ
dropMessageInHBQ(MessageTuple, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit) ->
  {MsgNr, Message} = MessageTuple,
  NewMessage = lists:concat([Message, " S in HBQ at ", werkzeug:timeMilliSecond()]),
  logToFile(werkzeug:list2String(["Message stored in HBQ: ", NewMessage])),
  NewHBQ = werkzeug:pushSL(HBQ, {MsgNr, NewMessage}),
  case werkzeug:lengthSL(NewHBQ) > DLQLimit / 2 of
    true ->
      pushMessagesInDLQ(NewHBQ, DLQ, ClientList, ClientLifetime, DLQLimit, true);
    _ -> queueMgmt(DLQLimit, NewHBQ, DLQ, ClientList, ClientLifetime)
  end
.

%% function to push the messages from the HBQ into the DLQ
pushMessagesInDLQ(HBQ, DLQ, ClientList, ClientLifetime, DLQLimit, NewPushingFlac) ->
  case werkzeug:notemptySL(HBQ) of
    false ->
      queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime);
    _ -> doNothing
  end,

  DLQisNotEmptyFlac = werkzeug:notemptySL(DLQ),
  MaxNrDLQ = werkzeug:maxNrSL(DLQ),
  MinNrHBQ = werkzeug:minNrSL(HBQ),
  MaxNrDLQplusTwo = MaxNrDLQ + 2,
  MaxNrDLQplusOne = MaxNrDLQ + 1,
  MinNrHBQminusOne = MinNrHBQ - 1,

%% missing messages -> failuremessage will be created and pushed in DLQ
  case MaxNrDLQplusTwo =< MinNrHBQ of
    true when NewPushingFlac andalso DLQisNotEmptyFlac ->
      FailMessage = lists:concat(["*** missing messages from ", MaxNrDLQplusOne, " to ", MinNrHBQminusOne, " at ", werkzeug:timeMilliSecond(), " ***"]),
      logToFile(lists:concat([werkzeug:timeMilliSecond(), ":  ", " FailMessage created - ", FailMessage])),
      case isDLQFull(DLQ, DLQLimit) of
        true ->
          NewDLQ = werkzeug:popSL(DLQ),
          NewPushedDLQ = werkzeug:pushSL(NewDLQ, {MinNrHBQminusOne, FailMessage}),
          pushMessagesInDLQ(HBQ, NewPushedDLQ, ClientList, ClientLifetime, DLQLimit, false);
        _ ->
          NewDLQ = werkzeug:pushSL(DLQ, {MinNrHBQminusOne, FailMessage}),
          pushMessagesInDLQ(HBQ, NewDLQ, ClientList, ClientLifetime, DLQLimit, false)
      end;

%% next missing messages in HBQ, stop pushing
    true when not NewPushingFlac andalso DLQisNotEmptyFlac ->
      queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime);

%% no missing messages
    _ ->
      case isDLQFull(DLQ, DLQLimit) of
        true ->
          NewDLQ = werkzeug:popSL(DLQ),
          {MsgNumber, Message} = werkzeug:findSL(HBQ, werkzeug:minNrSL(HBQ)),
          NewHBQ = werkzeug:popSL(HBQ),
          NewMessage = lists:concat([Message, " S in DLQ at ", werkzeug:timeMilliSecond()]),
          logToFile(werkzeug:list2String(["Message stored in DLQ: ", NewMessage])),
          NewPushedDLQ = werkzeug:pushSL(NewDLQ, {MsgNumber, NewMessage}),
          pushMessagesInDLQ(NewHBQ, NewPushedDLQ, ClientList, ClientLifetime, DLQLimit, false);
        _ ->
          {MsgNumber, Message} = werkzeug:findSL(HBQ, werkzeug:minNrSL(HBQ)),
          NewHBQ = werkzeug:popSL(HBQ),
          NewMessage = lists:concat([Message, " S in DLQ at ", werkzeug:timeMilliSecond()]),
          logToFile(werkzeug:list2String(["Message stored in DLQ: ", NewMessage])),
          NewDLQ = werkzeug:pushSL(DLQ, {MsgNumber, NewMessage}),
          pushMessagesInDLQ(NewHBQ, NewDLQ, ClientList, ClientLifetime, DLQLimit, false)
      end
  end
.

%% checks if the DLQ is full
isDLQFull(DLQ, DLQLimit) ->
  case werkzeug:lengthSL(DLQ) == DLQLimit of
    true -> true;
    _ -> false
  end
.
%% --------- ending the algo ----------

%% sending the next message to the client
sendMessageToClient(ClientPID, LastSendMessagenumber, DLQ) ->
  Result = werkzeug:findneSL(DLQ, LastSendMessagenumber),
  case Result of
%% no (new) message in DLQ to deliver to the client
    {-1, nok} when LastSendMessagenumber >= 2 ->
      ClientPID ! {reply, LastSendMessagenumber, lists:concat(["dummy message - no new messages in DLQ until yet (", werkzeug:timeMilliSecond(), ")"]), true},
      LastSendMessagenumber;

    {-1, nok} when LastSendMessagenumber =< 1 ->
      MinDLQ = werkzeug:minNrSL(DLQ),
      {SNr, Elem} = werkzeug:findSL(DLQ, MinDLQ),
      NewMessage = lists:concat([Elem, " S Out: ", werkzeug:timeMilliSecond()]),
      logToFile(werkzeug:list2String(["Message send to client: ", NewMessage])),
      ClientPID ! {reply, SNr, NewMessage, false},
      SNr + 1;

%% next message found, sending to client
    {SNr, Elem} ->
      case SNr == werkzeug:maxNrSL(DLQ) of
        true ->
          NewMessage = lists:concat([Elem, " S Out: ", werkzeug:timeMilliSecond()]),
          logToFile(werkzeug:list2String(["Message send to client: ", NewMessage])),
          ClientPID ! {reply, SNr, NewMessage, true},
          SNr + 1;
        _ ->
          NewMessage = lists:concat([Elem, " S Out: ", werkzeug:timeMilliSecond()]),
          logToFile(werkzeug:list2String(["Message send to client: ", NewMessage])),
          ClientPID ! {reply, SNr, NewMessage, false},
          SNr + 1
      end
  end
.

%% getting the last messagenumber which was send to the client
clientMgmt_lastMessageNumberSend(ClientPID, ClientList) ->
  ClientPIDTuple = [{Elem, LastMessageNumberSend} || {Elem, LastMessageNumberSend, _} <- ClientList, Elem == ClientPID],
  case ClientPIDTuple == [] of
    true -> 0;
    _ ->
      [{_, MessageNumber}] = ClientPIDTuple,
      MessageNumber
  end
.

%% adding a new messagenumber and starting a new timer for the client
clientMgmt_newMessageNumberOfClient(ClientPID, LastMessagenumberSend, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit) ->
  ClientElem = [{Elem, LMNS, Timer} || {Elem, LMNS, Timer} <- ClientList, Elem == ClientPID],
  case ClientElem == [] of
    false ->
      [{_, _, Timer}] = ClientElem,
      [ClientVar] = ClientElem,
      NewTimer = werkzeug:reset_timer(Timer, ClientLifetime, {endOfClientLifeTime, ClientPID}),
      DelClientList = lists:delete(ClientVar, ClientList),
      NewClientList = DelClientList ++ [{ClientPID, LastMessagenumberSend, NewTimer}],
      queueMgmt(DLQLimit, HBQ, DLQ, NewClientList, ClientLifetime);

    true ->
      {ok, Timer} = timer:send_after(ClientLifetime, {endOfClientLifeTime, ClientPID}),
      NewClientList = ClientList ++ [{ClientPID, LastMessagenumberSend, Timer}],
      queueMgmt(DLQLimit, HBQ, DLQ, NewClientList, ClientLifetime)
  end
.

%% deleting the client out of the ClientList if the client timeouts
clientMgmt_deleteClient(ClientPID, HBQ, DLQ, ClientList, ClientLifetime, DLQLimit) ->
  ClientElem = [{Elem, LMNS, Timer} || {Elem, LMNS, Timer} <- ClientList, Elem == ClientPID],
  case ClientElem == [] of
    true -> queueMgmt(DLQLimit, HBQ, DLQ, ClientList, ClientLifetime);
    _ ->
      [ClientVar] = ClientElem,
      NewClientList = lists:delete(ClientVar, ClientList),
      queueMgmt(DLQLimit, HBQ, DLQ, NewClientList, ClientLifetime)
  end

.


exitQueueMgmt() ->
  io:format("INFO: Server-QueueMgmt with PID: ~p ended at ~p~n", [self(), werkzeug:timeMilliSecond()]),
  erlang:exit("EndOfLife")
.


%% -- helping classes --
getHostname() ->
  {ok, Hostname} = inet:gethostname(),
  Hostname
.

logToFile(Message) ->
  Filename = lists:concat(["ServerQueueMgmt: ", "@", getHostname(), ".log"]),
  werkzeug:logging(Filename, lists:concat(["[", werkzeug:timeMilliSecond(), "] ", ": ", Message, "\n"]))
.