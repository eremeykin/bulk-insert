package pete.eremeykin.bulkinsert.startup;

import lombok.RequiredArgsConstructor;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.shell.standard.commands.Help;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
class StartupHelpPrinter {
    private final Help helpCommand;

    @EventListener
    public void onStartup(ContextRefreshedEvent event) throws Exception {
        String helpText = helpCommand.help(null).toString();
        System.out.println(helpText);
    }
}
