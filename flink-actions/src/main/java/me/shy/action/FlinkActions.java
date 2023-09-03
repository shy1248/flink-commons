package me.shy.action;

import java.util.Optional;

/**
 * @author shy
 * @date 2023/09/02 13:38
 **/
public class FlinkActions {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            ActionFactory.showDefaultHelp();
            System.exit(1);
        }
        Optional<Action> action = ActionFactory.createAction(args);
        if (action.isPresent()) {
            action.get().run();
        } else {
            System.exit(1);
        }
    }
}
