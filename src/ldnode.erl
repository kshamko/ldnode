%%%-------------------------------------------------------------------
%%% @author konstantin.shamko
%%% @copyright (C) 2015, Konstantin Shamko <konstantin.shamko@gmail.com>
%%% @doc
%%%
%%% @end
%%% Created : 07. Dec 2015 2:42 PM
%%%-------------------------------------------------------------------
-module(ldnode).
-author("konstantin.shamko").

-behaviour(gen_fsm).

-callback start_link(Params::list()) -> ok.
-callback start_leader() -> ok.
-callback handle_slave_down() -> ok.
-callback handle_leader(Event::atom()) -> ok.
-callback handle_slave(Event::atom()) -> ok.

%% API
-export([start_link/2, start_leader/1, send_event_leader/2, send_event_slave/2]).

%% States
-export([leader/2, slave/2]).

%% gen_fsm callbacks
-export([init/1,
  handle_event/3,
  handle_sync_event/4,
  handle_info/3,
  terminate/3,
  code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {module, leader_node, el_votes = 0, el_min_proc, el_node}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @end
%%--------------------------------------------------------------------
start_link(ProcName, Mod) ->
  gen_fsm:start_link({local,ProcName}, ?MODULE, [Mod], []).

start_leader(ProcName) ->
  gen_fsm:send_all_state_event(ProcName, {start_leader, ProcName}).

send_event_leader(Proc, Event) ->
  send_event(Proc, leader, Event).

send_event_slave(Proc, Event) ->
  send_event(Proc, slave, Event).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @end
%%--------------------------------------------------------------------
init([Mod]) ->
  {ok, slave, #state{module = Mod}}.


%%-------------------------------------
%% State
%%-------------------------------------
leader({behaviour_callback, leader, Message}, State) ->
  Mod = State#state.module,
  {_Result, NewState} = Mod:handle_leader(Message, State),
  {next_state, leader, NewState};
leader(_Event, State) ->
  {next_state, leader, State}.

%%-------------------------------------
%%
%%-------------------------------------
slave(election_begin, State) ->
  rpc:abcast(nodes(), ?MODULE, {proc_count, node(), erlang:system_info(process_count)}),
  {next_state, slave, State};

slave({behaviour_callback, slave, Message}, State) ->
  Mod = State#state.module,
  {_Result, NewState} = Mod:handle_slave(Message, State),
  {next_state, slave, NewState};

slave(_Event, State) ->
  {next_state, slave, State}.


-spec(handle_info(Info :: term(), StateName :: atom(),
    StateData :: term()) ->
  {next_state, NextStateName :: atom(), NewStateData :: term()} |
  {next_state, NextStateName :: atom(), NewStateData :: term(),
    timeout() | hibernate} |
  {stop, Reason :: normal | term(), NewStateData :: term()}).

%%---------------------------------------
%% Notification that cluster has a new leader
%%---------------------------------------
handle_info({new_leader, LeaderNode}, _StateName, State) ->
  lager:log(info, self(), "New leader ~p", [LeaderNode]),
  erlang:monitor_node(LeaderNode, true),
  {next_state, slave, State#state{leader_node = LeaderNode}};

%%---------------------------------------
%% Notification that leader node is dead
%%---------------------------------------
handle_info({nodedown, DeadLeaderNode}, slave, State) ->
  lager:log(info, self(), "Leader died. Election starts."),

  case nodes() of
    [] ->
      {next_state, leader, State#state{leader_node = node()}};
    _ ->
      {registered_name, ProcName} = erlang:process_info(self(), registered_name),

      ProcCount = erlang:system_info(process_count),

      notify_nodes(
        ProcName,
        {
          election_start,
          node(),
          ProcCount,
          ProcName,
          DeadLeaderNode
        }
      ),
      {next_state, slave, State#state{leader_node = null, el_node=node(), el_min_proc=ProcCount, el_votes = 0}}
  end;
%%---------------------------------------
%% Election Begins
%%---------------------------------------
handle_info({election_start, Node, ProcCount, ProcName, DeadLeaderNode}, slave, State) ->

  lager:log(info, self(), "Election response from ~p with proc count of ~p", [Node, ProcCount]),

  CountVotes = State#state.el_votes + 1,
  {CurLeader, CountProc} = get_current_leader({State#state.el_node, State#state.el_min_proc}, {Node, ProcCount}),

  all_votes_received(
    CountVotes,
    erlang:length(nodes()),
    CurLeader,
    ProcName,
    DeadLeaderNode
  ),

  {next_state, slave, State#state{el_min_proc = CountProc, el_node = CurLeader, el_votes = CountVotes}};
%%---------------------------------------
%% Election Finished
%%---------------------------------------
handle_info({election_finish, ProcName, DeadLeaderNode}, slave, State) ->
  lager:log(info, self(), "New leader elected ~p", [node()]),
  start_leader(ProcName),
  send_event_leader(ProcName, {new_leader, {old_leader, DeadLeaderNode}}),
  {next_state, slave, State};

%%---------------------------------------
%% Slave death handling
%%---------------------------------------
handle_info({nodedown, SlaveNode}, leader, State) ->
  Mod = State#state.module,
  {NextState, NewState} = Mod:handle_slave_down(SlaveNode, State),
  {next_state, NextState, NewState};
%%---------------------------------------
%% Process all other messages
%%---------------------------------------
handle_info(_Info, StateName, State) ->
  {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_event(Event :: term(), StateName :: atom(),
    StateData :: #state{}) ->
  {next_state, NextStateName :: atom(), NewStateData :: #state{}} |
  {next_state, NextStateName :: atom(), NewStateData :: #state{},
    timeout() | hibernate} |
  {stop, Reason :: term(), NewStateData :: #state{}}).

handle_event({start_leader, ProcName}, _StateName, State) ->
  notify_nodes(ProcName, {new_leader, node()}),
  [monitor_node(Node, true) || Node <- nodes()],
  {next_state, leader, State#state{leader_node = node(), el_votes = 0}};
handle_event(_Event, StateName, State) ->
  {next_state, StateName, State}.






%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_sync_event(Event :: term(), From :: {pid(), Tag :: term()},
    StateName :: atom(), StateData :: term()) ->
  {reply, Reply :: term(), NextStateName :: atom(), NewStateData :: term()} |
  {reply, Reply :: term(), NextStateName :: atom(), NewStateData :: term(),
    timeout() | hibernate} |
  {next_state, NextStateName :: atom(), NewStateData :: term()} |
  {next_state, NextStateName :: atom(), NewStateData :: term(),
    timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewStateData :: term()} |
  {stop, Reason :: term(), NewStateData :: term()}).
handle_sync_event(_Event, _From, StateName, State) ->
  Reply = ok,
  {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: normal | shutdown | {shutdown, term()}
| term(), StateName :: atom(), StateData :: term()) -> term()).
terminate(_Reason, _StateName, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, StateName :: atom(),
    StateData :: #state{}, Extra :: term()) ->
  {ok, NextStateName :: atom(), NewStateData :: #state{}}).
code_change(_OldVsn, StateName, State, _Extra) ->
  {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------
notify_nodes(ProcName, Message) ->
  rpc:abcast(nodes(), ProcName, Message).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------
all_votes_received(CountVotes, NodesCount, NewLeader, ProcName, DeadLeaderNode) when CountVotes >= NodesCount ->
  rpc:abcast([NewLeader], ProcName, {election_finish, ProcName, DeadLeaderNode});
all_votes_received(_CountVotes, _NodesCount, _NewLeader, _ProcName, _DeadLeaderNode) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------
get_current_leader({CurNode, CurProcCount}, {CandNode, CandProcCount}) when CurProcCount == CandProcCount ->
  case CurNode < CandNode of
    true -> {CandNode, CandProcCount};
    false -> {CurNode, CurProcCount}
  end;
get_current_leader({_CurNode, CurProcCount}, {CandNode, CandProcCount}) when CurProcCount > CandProcCount ->
  {CandNode, CandProcCount};
get_current_leader({CurNode, CurProcCount}, {_CandNode, _CandProcCount})->
  {CurNode, CurProcCount}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------
send_event(ProcName, StateName, Message) ->
  gen_fsm:send_event(ProcName, {behaviour_callback, StateName, Message}).

