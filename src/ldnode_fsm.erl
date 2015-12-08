%%%-------------------------------------------------------------------
%%% @author konstantin.shamko
%%% @copyright (C) 2015, Oxagile LLC
%%% @doc
%%%
%%% @end
%%% Created : 07. Dec 2015 2:42 PM
%%%-------------------------------------------------------------------
-module(ldnode_fsm).
-author("konstantin.shamko").

-behaviour(gen_fsm).

%% API
-export([start_link/0, election_begin/0, make_leader/0]).

%% States
-export([leader/2, dude/2]).

%% gen_fsm callbacks
-export([init/1,
  state_name/2,
  state_name/3,
  handle_event/3,
  handle_sync_event/4,
  handle_info/3,
  terminate/3,
  code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {leader_node, el_votes = 0, el_min_proc, el_node}).

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
-spec(start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], []).

make_leader() ->
  gen_fsm:send_event(?MODULE, make_leader).

election_begin() ->
  gen_fsm:send_event(?MODULE, election_begin).


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
init([]) ->
  {ok, dude, #state{}}.


%%-------------------------------------
%%
%%-------------------------------------
leader(_Event, State) ->
  {next_state, leader, State}.

%%-------------------------------------
%%
%%-------------------------------------
dude(make_leader, State) ->
  notify_nodes({leader, node()}),
  {next_state, leader, State#state{leader_node = node(), el_votes = 0}};

dude(election_begin, State) ->
  rpc:abcast(nodes(), ?MODULE, {proc_count, node(), erlang:system_info(process_count)}),
  {next_state, dude, State}.


%dude({leader_node, Node}, State) ->
%  io:format("Leader node ~p~n", [Node]),
%  erlang:monitor_node(Node, true),
%  {next_state, dude, State#state{leader_node = Node}};
%dude({task_from_leader, Data}, State) ->
%  {next_state, leader, State};
%dude(_Event, State) ->
%  {next_state, dude, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: term(), StateName :: atom(),
    StateData :: term()) ->
  {next_state, NextStateName :: atom(), NewStateData :: term()} |
  {next_state, NextStateName :: atom(), NewStateData :: term(),
    timeout() | hibernate} |
  {stop, Reason :: normal | term(), NewStateData :: term()}).

handle_info({nodedown, _LeaderNode}, dude, State) ->
  case nodes() of
    [] ->
      {next_state, leader, State#state{leader_node = node()}};
    _ ->
      notify_nodes({election_start, node(), erlang:system_info(process_count)}),
      {next_state, dude, State#state{leader_node = null}}
  end;

handle_info({election_start, Node, ProcCount}, dude, State) ->

  io:format("Election msg ~n"),

  CountVotes = State#state.el_votes + 1,
  {CurLeader, CountProc} =
    case ProcCount < State#state.el_min_proc of
      true -> {Node, ProcCount};
      false -> {State#state.el_node, State#state.el_min_proc}
    end,

  all_votes_received(
    CountVotes,
    erlang:length(nodes()),
    CurLeader
  ),

  {next_state, dude, State#state{el_min_proc = CountProc, el_node = CurLeader, el_votes = CountVotes}};

handle_info({leader, LeaderNode}, _StateName, State) ->
  erlang:monitor_node(LeaderNode, true),
  {next_state, dude, State#state{leader_node = LeaderNode}};

handle_info(you_are_leader, dude, State) ->
  io:format("You are leader ~n"),
  make_leader(),
  {next_state, dude, State};

handle_info(Info, StateName, State) ->
  io:format("Got sys event ~p ~p~n", [Info, StateName]),
  {next_state, StateName, State}.



%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @end
%%--------------------------------------------------------------------
-spec(state_name(Event :: term(), State :: #state{}) ->
  {next_state, NextStateName :: atom(), NextState :: #state{}} |
  {next_state, NextStateName :: atom(), NextState :: #state{},
    timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
state_name(_Event, State) ->
  {next_state, state_name, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @end
%%--------------------------------------------------------------------
-spec(state_name(Event :: term(), From :: {pid(), term()},
    State :: #state{}) ->
  {next_state, NextStateName :: atom(), NextState :: #state{}} |
  {next_state, NextStateName :: atom(), NextState :: #state{},
    timeout() | hibernate} |
  {reply, Reply, NextStateName :: atom(), NextState :: #state{}} |
  {reply, Reply, NextStateName :: atom(), NextState :: #state{},
    timeout() | hibernate} |
  {stop, Reason :: normal | term(), NewState :: #state{}} |
  {stop, Reason :: normal | term(), Reply :: term(),
    NewState :: #state{}}).
state_name(_Event, _From, State) ->
  Reply = ok,
  {reply, Reply, state_name, State}.

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
notify_nodes(Message) ->
  rpc:abcast(nodes(), ?MODULE, Message).

all_votes_received(CountVotes, NodesCount, NewLeader) when CountVotes >= NodesCount ->
  rpc:abcast([NewLeader], ?MODULE, you_are_leader);
all_votes_received(_CountVotes, _NodesCount, _NewLeader) ->
  ok.
