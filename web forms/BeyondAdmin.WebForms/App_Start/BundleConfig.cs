using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Optimization;
using System.Web.UI;

namespace BeyondAdmin.WebForms
{
    public class BundleConfig
    {
        public static void RegisterBundles(BundleCollection bundles)
        {
            bundles.Add(new StyleBundle("~/css/bootstrap").Include(
                "~/assets/css/bootstrap.min.css"));

            bundles.Add(new StyleBundle("~/css/bootstrap-rtl").Include(
                "~/assets/css/bootstrap-rtl.min.css"));


            bundles.Add(new StyleBundle("~/css/beyond").Include(
                "~/assets/css/beyond.min.css",
                "~/assets/css/demo.min.css",
                "~/assets/css/font-awesome.min.css",
                "~/assets/css/typicons.min.css",
                "~/assets/css/weather-icons.min.css",
                "~/assets/css/animate.min.css"
                ));

            bundles.Add(new StyleBundle("~/css/beyond-rtl").Include(
                "~/assets/css/beyond-rtl.min.css",
                "~/assets/css/demo.min.css",
                "~/assets/css/font-awesome.min.css",
                "~/assets/css/typicons.min.css",
                "~/assets/css/weather-icons.min.css",
                "~/assets/css/animate.min.css"
                ));

            bundles.Add(new ScriptBundle("~/bundles/skin").Include(
                "~/assets/js/skins.min.js"));

            bundles.Add(new ScriptBundle("~/bundles/jquery").Include(
                "~/assets/js/jquery.min.js"));

            bundles.Add(new ScriptBundle("~/bundles/bootstrap").Include(
                "~/assets/js/bootstrap.min.js",
                "~/assets/js/slimscroll/jquery.slimscroll.min.js"
                ));

            bundles.Add(new ScriptBundle("~/bundles/beyond").Include(
                "~/assets/js/beyond.min.js"));

            bundles.Add(new ScriptBundle("~/bundles/jqueryval").Include(
                        "~/assets/js/jqueryval/jquery.validate*"));
        }
    }
}